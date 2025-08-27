import pandas as pd
import json
import streamlit as st
import os
import re
import numpy as np
from sklearn.metrics import accuracy_score, precision_score, f1_score
from sklearn.linear_model import LinearRegression
from collections import defaultdict

# Adjust path as needed to import your modules
from services import azure_storage

st.markdown(
    """
    <style>
    /* Make the main container a bit narrower */
    .main > div {
        max-width: 800px;
    }
    /* Add subtle styling to text areas */
    .stTextArea textarea {
        border: 1px solid #ddd;
        border-radius: 6px;
    }
    /* Center-align the success/info/warning messages */
    .element-container {
        margin-left: auto;
        margin-right: auto;
    }
    </style>
    """,
    unsafe_allow_html=True
)

############################
# 1. Helper Functions
############################
def flatten_json(nested_json, parent_key='', sep='.'):
    """
    Recursively flattens a nested JSON/dict.
    E.g. {"Key1": {"SubKey1": "val1", "SubKey2": "val2"}, "Key2": true}
    becomes {"Key1.SubKey1": "val1", "Key1.SubKey2": "val2", "Key2": true}
    """
    items = []
    for k, v in nested_json.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_json(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def aggregate_data(json_list):
    """
    Flatten each JSON and collect values in a dict of lists:
        {
          "Key1.SubKey1": [val1, val2, ...],
          "Key2": [val3, val4, ...],
          ...
        }
    """
    aggregated = defaultdict(list)
    for j in json_list:
        flat_j = flatten_json(j)
        for key, val in flat_j.items():
            aggregated[key].append(val)
    return aggregated

def convert_value(x):

    # Return booleans unchanged.
    if isinstance(x, bool):
        return x

    # Leave integers unchanged.
    if isinstance(x, int):
        return x

    # Process floats: only 1.0 and 0.0 are recognized.
    if isinstance(x, float):
        if x == 1.0:
            return True
        elif x == 0.0:
            return False
        else:
            return None

    # Process strings.
    if isinstance(x, str):
        s = x.strip().lower()
        mapping = {"yes": True, "true": True, "no": False, "false": False}
        if s in mapping:
            return mapping[s]
        # Also allow strings that are numeric representations.
        try:
            num = int(s)
            if num == 1:
                return True
            elif num == 0:
                return False
        except ValueError:
            return None
        return None

    # For other types, attempt to convert to a string and process.
    try:
        s = str(x).strip().lower()
    except Exception:
        return None
    if s in ("yes", "true"):
        return True
    elif s in ("no", "false"):
        return False
    return None

def get_eval_data(selected_prompt_name):
    # Collect available filenames (just the base filenames, e.g. AC-service5.json)
    llm_analysis = [f for f in (azure_storage.list_llmanalysis(selected_prompt_name) or []) if f.endswith('.json')]
    eval_files = [f for f in (azure_storage.list_evals(selected_prompt_name) or []) if f.endswith('.json')]

    if not llm_analysis:
        st.warning("No LLM analysis files found for this persona. Generate analyses first from 'Personas and GenAI'.")
        return []
    if not eval_files:
        st.warning("No ground-truth files found for this persona. Upload ground truth in this page.")
        return []

    # Match by exact filename
    common = sorted(set(llm_analysis).intersection(set(eval_files)))
    if not common:
        # Help the user diagnose mismatched names
        st.warning("No filename matches between analysis and ground truth. Ensure 'Call ID' in CSV matches the audio basename used in analysis.")
        st.info(f"Examples - analysis only: {', '.join(sorted(set(llm_analysis) - set(eval_files))[:5])}{' …' if len(set(llm_analysis) - set(eval_files))>5 else ''}")
        st.info(f"Examples - ground-truth only: {', '.join(sorted(set(eval_files) - set(llm_analysis))[:5])}{' …' if len(set(eval_files) - set(llm_analysis))>5 else ''}")
        return []

    all_jsons = []
    for file in common:
        try:
            data = azure_storage.read_llm_analysis(selected_prompt_name, file)
            ground_truth = azure_storage.read_eval(selected_prompt_name, file)
            # Fallback for mistakenly uploaded evals with an extra .json suffix
            if (not ground_truth or len(ground_truth.keys()) == 0) and file.endswith('.json'):
                alt_name = f"{file}.json"
                ground_truth = azure_storage.read_eval(selected_prompt_name, alt_name)
            if not ground_truth:
                continue
            merged = dict(data) if isinstance(data, dict) else {}
            for key, value in ground_truth.items():
                if isinstance(key, str) and key.lower() == "call id":
                    continue
                merged[f"{key}.gt"] = value
            all_jsons.append(merged)
        except Exception as e:
            st.error(f"Error reading {file}: {e}")
    return all_jsons

def get_prediction_data(selected_prompt_name):
    """
    Load ALL analysis JSONs for the persona (no ground-truth required).
    Adds a 'Call ID' field derived from the filename.
    Returns a list of dictionaries (one per call).
    """
    analysis_files = [f for f in (azure_storage.list_llmanalysis(selected_prompt_name) or []) if f.endswith('.json')]
    results = []
    for file in analysis_files:
        try:
            data = azure_storage.read_llm_analysis(selected_prompt_name, file)
            call_id = os.path.splitext(file)[0]
            if isinstance(data, dict):
                data = {**data, "Call ID": call_id}
            else:
                data = {"Call ID": call_id}
            results.append(data)
        except Exception as e:
            st.error(f"Error reading analysis {file}: {e}")
    return results

############################
# 1. UI
############################

st.header("1. ⚙️ Scoring Parameters")
#add some help here to explain that those KPIs should align with the JSON defined in the personas to be extracted
st.markdown("Define the KPIs/Parameters that will be extracted from the evaluation files.")

if "kpis" not in st.session_state:
    st.session_state["kpis"] = []

prompt_files = azure_storage.list_prompts()
if not prompt_files:
    st.warning("No persona files found in the container.")
    st.stop()

selected_eval_prompt = st.selectbox("Select a Persona", prompt_files)
if not selected_eval_prompt:
    st.info("Select a persona from the dropdown above to continue.")
    st.stop()
else:
    existing_config = azure_storage.read_prompt_config(selected_eval_prompt)
    if existing_config:
        st.session_state["kpis"] = existing_config
    else:
        st.session_state["kpis"] = []
    
# --- UI for adding a new KPI ---
with st.expander("Add or Update a KPI Parameter", expanded=False):
    kpi_name = st.text_input("KPI Name", value="", max_chars=100)
    add_kpi_col, _ = st.columns([1, 3])
    with add_kpi_col:
        if st.button("Add ground truth KPI", help="Click to add/update a KPI parameter."):
            if not kpi_name.strip():
                st.error("KPI name cannot be empty.")
            else:
                st.session_state["kpis"].append(kpi_name)
                azure_storage.upload_prompt_config(selected_eval_prompt, st.session_state["kpis"])
                st.success(f"KPI '{kpi_name}' added/updated.")


# --- Display current KPIs ---
if st.session_state["kpis"] and len(st.session_state["kpis"]) > 0:
    st.write("**Current KPIs/Parameters**:")
    # A simple table with remove buttons
    for value in list(st.session_state["kpis"]):
        remove_col, text_col = st.columns([0.1, 0.9])
        with remove_col:
            if st.button("❌", key=f"remove_{value}", help=f"Remove {value}"):
                st.session_state["kpis"].remove(value)
                azure_storage.upload_prompt_config(selected_eval_prompt, st.session_state["kpis"])
        with text_col:
            st.write(value)
else:
    st.info("No KPIs defined yet. Add at least one above.")
    st.stop()

st.markdown("---")

st.title("2. 📊 Ground truth")
st.markdown("Upload an evaluation (CSV/XLSX) containing the KPIs defined for a given prompt.")

# Attempt to download the config from the same container
config_data = azure_storage.read_prompt_config(selected_eval_prompt)
if config_data is None:
    st.error(f"Could not find a config file for prompt '{selected_eval_prompt}'. Please define KPIs first.")
    st.stop()

required_columns = list(config_data) + ["Call ID (or audio_basename)"]
st.write(f"**Required columns** for this evaluation file: {required_columns}")

uploaded_eval_file = st.file_uploader(
    f"Upload your evaluation file for '{selected_eval_prompt}' (CSV/XLSX)",
    type=["csv", "xlsx"]
)

if uploaded_eval_file is not None:
    # Read file into DataFrame
    try:
        if uploaded_eval_file.name.endswith(".csv"):
            df_eval = pd.read_csv(uploaded_eval_file)
        else:
            df_eval = pd.read_excel(uploaded_eval_file, sheet_name='Parameters')
    except Exception as e:
        st.error(f"Error reading file: {e}")
        st.stop()

    # Check required KPI columns only; allow either 'Call ID' or 'audio_basename' for filename
    missing_kpis = [col for col in list(config_data) if col not in df_eval.columns]
    if missing_kpis:
        st.error(f"Missing required KPI columns in your file: {missing_kpis}")
        st.stop()
    filename_col = "audio_basename" if "audio_basename" in df_eval.columns else ("Call ID" if "Call ID" in df_eval.columns else None)
    if filename_col is None:
        st.error("Missing required filename column. Add either 'audio_basename' or 'Call ID' to your file.")
        st.stop()

    # Upload each row as JSON
    success_count = 0
    for idx, row in df_eval.iterrows():
        row_dict = row.to_dict()
        name_value = str(row_dict.get(filename_col, "")).strip()
        if not name_value:
            st.warning(f"Skipping row {idx}: empty {filename_col}.")
            continue
        # Accept values with or without .json, keep basename only
        call_id = os.path.splitext(name_value)[0]

        if not call_id or call_id.lower() == "not found":
            st.warning(f"Skipping row {idx}: invalid Call ID.")
            continue

        blob_name = f"{call_id}.json"
        row_json = json.dumps(row_dict, indent=2)

        try:
            azure_storage.upload_eval_to_blob(blob_name, selected_eval_prompt, row_json)
            success_count += 1
        except Exception as e:
            st.error(f"Failed to upload row index {idx} (Call ID: {call_id}). Error: {e}")

    if success_count:
        st.success(f"Successfully uploaded {success_count} evaluation file(s) to storage.")

st.markdown("---")

st.title("3. 📈 Evaluation Results")
st.markdown("Evaluate the AI predictions against the ground truth data.")
numeric_pred_options = [
    "LLM predictions",
    "Computed sum baseline",
    "Linear regression (GT features)",
]
numeric_pred_strategy = st.selectbox(
    "Prediction source for numeric KPIs",
    numeric_pred_options,
    index=1,
    help="LLM uses values from analysis JSONs. Baselines compute predictions from ground-truth feature columns to benchmark quality.")

eval_data = get_eval_data(selected_eval_prompt)

# Aggregate all JSON data
if len(eval_data) == 0:
    st.stop()

aggregated = aggregate_data(eval_data)
df = pd.DataFrame({k: pd.Series(v) for k, v in aggregated.items()})
# Compute total rows robustly as the max length of any column
total_rows = int(max((len(v) for v in aggregated.values()), default=0))
st.markdown(f"**Total records that have ground truth**: {total_rows}")

# Build normalized lookup maps between KPI base names and actual columns
def _norm(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(name).lower())

pred_map = {}
gt_map = {}
for col in df.columns:
    if isinstance(col, str) and col.endswith(".score"):
        base = col[:-6]
        pred_map[_norm(base)] = col
    if isinstance(col, str) and col.endswith(".gt"):
        base = col[:-3]
        gt_map[_norm(base)] = col

available_pred = sorted({base for base in (c[:-6] for c in df.columns if isinstance(c, str) and c.endswith('.score'))})
available_gt = sorted({base for base in (c[:-3] for c in df.columns if isinstance(c, str) and c.endswith('.gt'))})

# Define the parameters to evaluate.
parameters = azure_storage.read_prompt_config(selected_eval_prompt) or []
cols = st.columns(len(parameters))

for i, param in enumerate(parameters):
    # Resolve columns by normalized matching to be robust to spaces/case/punctuation
    pred_col = pred_map.get(_norm(param))
    truth_col = gt_map.get(_norm(param))

    with cols[i]:
        st.write(f"### {param}")
        if not pred_col or not truth_col or pred_col not in df.columns or truth_col not in df.columns:
            st.warning(
                f"Missing data for '{param}'.\n\n"
                f"Expected predicted column like '<param>.score' and ground-truth '<param>.gt'.\n\n"
                f"Available predicted KPIs: {', '.join(available_pred[:8])}{' …' if len(available_pred)>8 else ''}.\n"
                f"Available ground-truth KPIs: {', '.join(available_gt[:8])}{' …' if len(available_gt)>8 else ''}."
            )
            continue
        # Try regression metrics first if both columns are numeric with variation
        y_true_num = pd.to_numeric(df[truth_col], errors='coerce')
        y_pred_num = pd.to_numeric(df[pred_col], errors='coerce')

        # Optional baselines for numeric KPIs
        if numeric_pred_strategy != "LLM predictions":
            # Build predictions from GT feature columns
            talk_col = next((c for c in df.columns if c.lower().endswith("talk_time_seconds.gt")), None)
            hold_col = next((c for c in df.columns if c.lower().endswith("hold_time_seconds.gt")), None)
            acw_col = next((c for c in df.columns if c.lower().endswith("after_call_work_seconds.gt")), None)
            checksum_col = next((c for c in df.columns if c.lower().endswith("computed_check_sum.gt")), None)

            if numeric_pred_strategy == "Computed sum baseline":
                if checksum_col is not None:
                    y_pred_num = pd.to_numeric(df[checksum_col], errors='coerce')
                elif all([talk_col, hold_col, acw_col]):
                    y_pred_num = pd.to_numeric(df[talk_col], errors='coerce') \
                                 + pd.to_numeric(df[hold_col], errors='coerce') \
                                 + pd.to_numeric(df[acw_col], errors='coerce')
            elif numeric_pred_strategy == "Linear regression (GT features)":
                feature_cols = [c for c in [talk_col, hold_col, acw_col] if c is not None]
                if not feature_cols and checksum_col is not None:
                    feature_cols = [checksum_col]
                if feature_cols:
                    X = pd.concat([pd.to_numeric(df[c], errors='coerce') for c in feature_cols], axis=1)
                    mask = (~X.isna().any(axis=1)) & (~y_true_num.isna())
                    if mask.sum() >= 3:
                        model = LinearRegression()
                        model.fit(X[mask].values, y_true_num[mask].values)
                        y_pred_series = pd.Series(model.predict(X.values), index=df.index)
                        y_pred_num = y_pred_series
        num_mask = (~y_true_num.isna()) & (~y_pred_num.isna())

        # If using LLM predictions but we don't have enough numeric pairs or they are all zero, auto-fallback to baselines
        if numeric_pred_strategy == "LLM predictions" and (num_mask.sum() < 3 or (y_pred_num[num_mask].fillna(0) == 0).all()):
            # try computed sum, then regression
            talk_col = next((c for c in df.columns if isinstance(c, str) and c.lower().endswith("talk_time_seconds.gt")), None)
            hold_col = next((c for c in df.columns if isinstance(c, str) and c.lower().endswith("hold_time_seconds.gt")), None)
            acw_col = next((c for c in df.columns if isinstance(c, str) and c.lower().endswith("after_call_work_seconds.gt")), None)
            checksum_col = next((c for c in df.columns if isinstance(c, str) and c.lower().endswith("computed_check_sum.gt")), None)
            if checksum_col is not None:
                y_pred_num = pd.to_numeric(df[checksum_col], errors='coerce')
            elif all([talk_col, hold_col, acw_col]):
                y_pred_num = pd.to_numeric(df[talk_col], errors='coerce') \
                             + pd.to_numeric(df[hold_col], errors='coerce') \
                             + pd.to_numeric(df[acw_col], errors='coerce')
            else:
                # last resort: regression if we have at least some features
                feature_cols = [c for c in [talk_col, hold_col, acw_col, checksum_col] if c is not None]
                if feature_cols:
                    X = pd.concat([pd.to_numeric(df[c], errors='coerce') for c in feature_cols], axis=1)
                    mask = (~X.isna().any(axis=1)) & (~y_true_num.isna())
                    if mask.sum() >= 3:
                        model = LinearRegression()
                        model.fit(X[mask].values, y_true_num[mask].values)
                        y_pred_series = pd.Series(model.predict(X.values), index=df.index)
                        y_pred_num = y_pred_series
        
        num_mask = (~y_true_num.isna()) & (~y_pred_num.isna())
        if num_mask.sum() >= 3:
            yt = y_true_num[num_mask].to_numpy(dtype=float)
            yp = y_pred_num[num_mask].to_numpy(dtype=float)
            mae = float(np.mean(np.abs(yp - yt)))
            rmse = float(np.sqrt(np.mean((yp - yt) ** 2)))
            denom = np.maximum(np.abs(yt), 1.0)
            within10 = float(np.mean(np.abs(yp - yt) <= 0.10 * denom) * 100.0)
            st.caption(f"Pairs used: {num_mask.sum()}")
            st.metric("Within 10%", f"{within10:.1f}%")
            st.metric("MAE", f"{mae:.2f}")
            st.metric("RMSE", f"{rmse:.2f}")
        else:
            # Fall back to classification metrics
            y_true = df[truth_col].apply(convert_value)
            y_pred = df[pred_col].apply(convert_value)

            valid_mask = y_true.notnull() & y_pred.notnull()
            y_true = y_true[valid_mask]
            y_pred = y_pred[valid_mask]

            if len(y_true) == 0:
                st.warning(f"No valid data for {param}")
            else:
                acc = accuracy_score(y_true, y_pred)
                if isinstance(y_true.iloc[0], bool) and isinstance(y_pred.iloc[0], bool):
                    prec = precision_score(y_true, y_pred, average='binary', pos_label=True, zero_division=0)
                    f1 = f1_score(y_true, y_pred, average='binary', pos_label=True, zero_division=0)
                else:
                    prec = precision_score(y_true, y_pred, average='weighted', zero_division=0)
                    f1 = f1_score(y_true, y_pred, average='weighted', zero_division=0)

                st.metric("Accuracy", f"{acc:.2f}")
                st.metric("Precision", f"{prec:.2f}")
                st.metric("F1 Score", f"{f1:.2f}")

st.markdown("---")

st.title("4. 🔮 Predictions (no ground truth)")
st.markdown("Review AI predictions even when ground truth is missing. Useful for QA and spot checks.")

pred_rows = get_prediction_data(selected_eval_prompt)
if not pred_rows:
    st.info("No predictions found. Generate analyses first from 'Personas and GenAI'.")
else:
    # Flatten and dataframe for display
    flat_preds = [flatten_json(r) for r in pred_rows]
    df_preds = pd.DataFrame(flat_preds)

    # Try to extract common KPI-style fields ending with .score
    score_cols = [c for c in df_preds.columns if isinstance(c, str) and c.endswith('.score')]
    nice_cols = ['Call ID'] + score_cols
    existing_cols = [c for c in nice_cols if c in df_preds.columns]
    st.dataframe(df_preds[existing_cols].fillna(""), use_container_width=True)
