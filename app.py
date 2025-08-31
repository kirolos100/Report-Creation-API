from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any
import json
import os
from datetime import datetime
from services import azure_storage, azure_transcription, azure_oai, azure_search
from flask import Flask, request, jsonify
import json
import requests
from bs4 import BeautifulSoup
from openai import AzureOpenAI
from flasgger import Swagger, swag_from
from flask_swagger_ui import get_swaggerui_blueprint
from flask_cors import CORS  # Import CORS

app = Flask(__name__)
CORS(app)  # Allow cross-origin requests
# CORS


# Health check endpoint
@app.route('/', methods=['GET'])
def read_root():
    return {"status": "healthy", "message": "API is running"}




SYSTEM_PROMPT_DEFAULT = (
    """You are a data analysis assistant. You will be provided with a transcript of a call-center conversation between a Customer and an Agent (the transcript may include timestamps in `HH:MM:SS`, `MM:SS`, or plain seconds). Your job is to analyze the call and **return one single valid JSON object only** (no surrounding text, no explanation, no extra JSON objects, no comments).

**Output schema (MUST be returned exactly — do not add, remove, rename, or omit any top-level keys or any nested keys shown below):**

```json
{
  "name": "<customer full name in English string or null if not present>",
  "summary": "<one paragraph consisting of exactly four sentences>",
  "sentiment": {
    "score": <integer 1-5>,
    "explanation": "<why this score was chosen>"
  },
  "main_issues": ["<issue1>", "<issue2>", "..."],
  "resolution": "<what the agent did or promised>",
  "additional_notes": "<optional extra notes>",
  "Average Handling Time (AHT)": {
    "score": <integer seconds>,
    "explanation": "<how you computed it; list components>"
  },
  "resolved": {
    "score": <true|false>,
    "explanation": "<why resolved is true/false>"
  },
  "disposition": {
    "score": "<one of: Resolved, Escalated, Pending, Wrong Number, Other>",
    "explanation": "<short explanation>"
  },
  "agent_professionalism": "<one of: Highly Professional, Professional, Needs Improvement>",
  "Call Generated Insights": {
    "Customer Sentiment": "<Positive|Neutral|Negative>",
    "Call Categorization": "<Inquiry|Product/Service|Issue|Other>",
    "Resolution Status": "<resolved|escalated|pending|other>",
    "Main Subject": "<short subject>",
    "Services": "<service(s) involved>",
    "Call Outcome": "<short outcome in one sentence>",
    "Agent Attitude": "<1–3 concise adjectives (dynamic per call) describing the agent's demeanor, e.g. Empathetic; Efficient and Professional; Rushed and Curt>",
    "summary": "<one paragraph consisting of exactly four sentences>",
  },
  "Customer Service Metrics": {
    "FCR": {
      "score": <true|false>,
      "explanation": "<did this call resolve the case on first contact?>"
    },
    "Talk time": <integer seconds>,
    "Hold time": <integer seconds>
  }
}
````

**Mandatory parsing & calculation rules (follow exactly):**

1. **Timestamps:** Parse timestamps in `HH:MM:SS`, `MM:SS`, or plain seconds; convert all to integer seconds before calculations. If timestamps are relative offsets, assume they are measured from call start — state this in any explanation that relies on it.

2. **Talk time (`Customer Service Metrics -> "Talk time"`):**

   * If utterances include start and end timestamps, compute each utterance duration as `end - start` and sum durations for Agent + Customer utterances.
   * If only start timestamps or offsets are available, infer utterance duration conservatively (document assumptions in the relevant `"explanation"` field).
   * Output MUST be an integer number of seconds (never 0).

3. **Hold time (`Customer Service Metrics -> "Hold time"`):**

   * Primary detection method (preferred): Detect explicit agent "please wait" utterances in the transcript text (Arabic and English). For Arabic transcripts, detect common agent waiting phrases and variants such as (but not limited to):
     "لحظة", "لحظات", "انتظر", "استنى", "استنّي", "خلي حضرتك", "هنرجع لك بعد شوي", "اسيبك على الانتظار", "معايا لحظة", "خلي حضرتك معايا لحظة", "معاك لحظة", "من فضلك انتظر", "هاخد منك لحظة", and obvious morphological variants or common colloquial spellings.
     When an agent utterance contains such a phrase and indicates an intended hold, treat the hold start as that utterance's timestamp. Treat the hold end as the timestamp when the agent next resumes speaking (i.e., next agent utterance start time) or explicitly announces the end of hold. Compute hold duration as hold\_end\_timestamp - hold\_start\_timestamp in integer seconds.
     If the agent issues a "please wait" phrase and there is no later agent timestamp in the transcript to mark resumption, you must not output 0 for Hold time. Instead:

     * If a later customer utterance exists, conservatively treat the earlier of (a) the next customer utterance start time or (b) a minimum conservative default hold duration of 5 seconds after the "please wait" timestamp — whichever yields the larger hold duration — and document this choice in the "explanation" field.
     * If the transcript has no subsequent timestamps at all, estimate a conservative default hold duration of 5 seconds, and explain the assumption.
   * If explicit markers are absent, infer hold from silence gaps between consecutive utterances where `gap >= 3 seconds` (gap = next\_utterance\_start − previous\_utterance\_end). Sum these inferred hold durations.
   * Output MUST be an integer number of seconds (never 0).
   * Prefer Arabic "please wait" detection and explicit markers over inferred silence. Always compute hold durations from timestamps and output as integer seconds (never 0). If exact timestamps are insufficient, estimate conservatively and explain assumptions concisely.
   * Important: Prefer Arabic "please wait" detection and explicit markers over silence inference. Always compute hold durations from timestamps and output integer seconds. Never output 0 for hold time if a "please wait" utterance is present — if timestamps are missing or incomplete for the hold, estimate conservatively and explain assumptions in the relevant "explanation" fields.

   Example application: For the agent utterance Agent: طبعا من خلالها لحظات معايا بعد اذنك واكد مع حضرتك الطلب. — if that utterance has timestamp 00:02:10, and the agent's next utterance resumes at 00:02:45, treat hold start=130s and hold end=165s and add 35 seconds to Hold time.

4. **AHT (Average Handling Time):**

   * `AHT (score)` must be an integer seconds equal to `Talk time + Hold time`.
   * Include component breakdown (e.g., `"talk_time: Xs, hold_time: Ys"`).

5. **Estimations & Transparency:**

   * **Never output 0** for any time metric. If exact computation is impossible, provide your best estimate and include the estimation method and assumptions inside the corresponding `"explanation"` field (one or two concise sentences).
   * Keep explanations short and precise.

6. **FCR / resolved / disposition:**

   * `resolved.score` is boolean; `FCR.score` is boolean. Explain reasoning briefly in their `"explanation"` fields.
   * `disposition.score` must be one of the allowed strings listed in the schema.
7.Name extraction:

* Detect and extract the customer’s full name in English if it appears in the transcript (introductions, agent confirmations, account details, voicemail tags, or other explicit mentions). Fill the top-level "name" field with the extracted full name string. If no clear customer name is present, set "name" to null.
8. Summary & Call Summary:
The "summary" field must be exactly one paragraph containing four sentences. Each sentence should be complete and concise; do not include lists, line breaks, or extra JSON objects inside this string.
9. Agent professionalism assessment:

Set "agent_professionalism" to one of exactly: "Highly Professional", "Professional", or "Needs Improvement". Base this on agent behavior (tone, helpfulness, adherence to procedure, politeness, clarity). Include brief justification where appropriate in related explanation fields (e.g., "Average Handling Time (AHT)" explanation or "additional_notes").

10. Agent attitude (dynamic):

For "Call Generated Insights" -> "Agent Attitude" select 1 to 3 concise adjectives or short phrases that best describe the agent's demeanor in this specific call (e.g., "Empathetic", "Efficient and Professional", "Rushed and Curt"). Do not use a fixed small set of static categories — choose descriptors dynamically based on the transcript evidence.

**Formatting & behavior rules:**

* Return **one and only one** valid JSON object and nothing else.
* Do not include any non-JSON text, logs, or metadata.
* All durations must be integers (seconds). All explanation strings should be concise (1–2 sentences).
* If the transcript lacks timestamps entirely, produce best-effort estimates for Talk time, Hold time, and AHT, explain assumptions in the relevant `"explanation"` fields, and still return non-zero integers for time fields.
* Do not change the schema: the JSON returned must include exactly the keys and nested keys listed above (you may change values, but not keys or structure).

**Example behavior (do not output this example in your response):**

* If the transcript includes explicit utterance start/end times, compute talk and hold precisely.
* If the transcript contains `[hold] 00:01:23 - 00:01:41`, add 18 seconds to Hold time.
* If gaps of 5–12 seconds exist and no hold markers are present, treat gaps ≥ 3 seconds as inferred hold.

You are responsible for correctly calculating and returning Talk time and Hold time (in seconds) and for producing the exact JSON structure above every time. If any part of the transcript is ambiguous, estimate conservatively, document the assumption in the appropriate `"explanation"` fields, and continue — but do not modify the JSON schema.
"""
)




def _parse_json_maybe(text: str) -> Any:
    try:
        return json.loads(text)
    except Exception:
        try:
            start = text.find("{")
            end = text.rfind("}") + 1
            if start != -1 and end != -1:
                return json.loads(text[start:end])
        except Exception:
            pass
    return {"raw": text}


def _first_analysis_for_call(call_id: str) -> tuple[Any | None, str | None]:
    """Return (analysis_obj, blob_path) for the first analysis JSON matching call_id under llmanalysis/**."""
    container = azure_storage.blob_service_client.get_container_client(azure_storage.DEFAULT_CONTAINER)
    prefix = f"{azure_storage.LLM_ANALYSIS_FOLDER}/"
    for blob in container.list_blobs(name_starts_with=prefix):
        if not blob.name.endswith(".json"):
            continue
        filename = blob.name.split("/")[-1]
        if filename.rsplit(".", 1)[0] == call_id:
            # strip folder prefix for read_blob
            rel = blob.name.split("/", 1)[1]
            txt = azure_storage.read_blob(rel, prefix=azure_storage.LLM_ANALYSIS_FOLDER)
            return (_parse_json_maybe(txt) if txt else None, blob.name)
    return (None, None)


def _persona_analysis_for_call(call_id: str) -> tuple[Any | None, str | None]:
    """Return (analysis_obj, blob_path) for persona folder specifically.
    Tries exact match, then normalized (spaces/underscores), then case-insensitive scan of persona dir.
    """
    container = azure_storage.blob_service_client.get_container_client(azure_storage.DEFAULT_CONTAINER)
    base_prefix = f"{azure_storage.LLM_ANALYSIS_FOLDER}/persona/"
    # 1) direct read
    for candidate in [call_id, call_id.replace(" ", "_"), call_id.replace("_", "-")]:
        rel = f"persona/{candidate}.json"
        txt = azure_storage.read_blob(rel, prefix=azure_storage.LLM_ANALYSIS_FOLDER)
        if txt:
            return (_parse_json_maybe(txt), f"{base_prefix}{candidate}.json")
    # 2) scan persona folder
    target_norm = call_id.lower().replace(" ", "_")
    for blob in container.list_blobs(name_starts_with=base_prefix):
        if not blob.name.endswith(".json"):
            continue
        fname = blob.name.split("/")[-1]
        fid = fname.rsplit(".", 1)[0]
        if fid.lower() == target_norm or fid.lower().replace("-", "_") == target_norm:
            rel = blob.name.split("/", 1)[1]
            txt = azure_storage.read_blob(rel, prefix=azure_storage.LLM_ANALYSIS_FOLDER)
            return (_parse_json_maybe(txt) if txt else None, blob.name)
    return (None, None)


def _get_ci(d: dict, keys: list[str]) -> Any:
    if not isinstance(d, dict):
        return None
    lower_map = {k.lower(): k for k in d.keys()}
    for k in keys:
        real = lower_map.get(k.lower())
        if real is None:
            continue
        val = d.get(real)
        # unwrap dict with score if present
        if isinstance(val, dict) and "score" in {x.lower() for x in val.keys()}:
            # case-insensitive access to score
            score_key = next((rk for rk in val.keys() if rk.lower() == "score"), None)
            if score_key:
                return val.get(score_key)
        return val
    return None


def _derive_category_and_attitude(analysis: Any) -> tuple[str | None, str | None]:
    if not isinstance(analysis, dict):
        return None, None
    # Prefer nested insights block if present
    insights: dict | None = None
    lower_map = {k.lower(): k for k in analysis.keys()}
    for k in ["Call Generated Insights", "call_generated_insights", "generated_insights", "insights"]:
        real = lower_map.get(k.lower())
        if real is not None and isinstance(analysis.get(real), dict):
            insights = analysis.get(real)  # type: ignore
            break
    if isinstance(insights, dict):
        cat_from_insights = _get_ci(
            insights,
            [
                "Call Categorization",
                "call_categorization",
                "Call Category",
                "call_category",
                "Main Subject",
                "subject",
                "Call Type",
            ],
        )  # type: ignore
        att_from_insights = _get_ci(
            insights,
            [
                "Agent Attitude",
                "agent_attitude",
                "Agent Behavior",
                "agent_behavior",
                "Agent Tone",
                "agent_tone",
                "Agents Professionalism",
                "professionalism",
            ],
        )  # type: ignore
        if cat_from_insights is not None or att_from_insights is not None:
            return (
                str(cat_from_insights) if cat_from_insights is not None else None,
                str(att_from_insights) if att_from_insights is not None else None,
            )
    # candidates for category
    category = _get_ci(
        analysis,
        [
            "Call Categorization",
            "call_categorization",
            "category",
            "call_category",
            "Main Subject",
            "subject",
            "Call Type",
        ],
    )
    # candidates for attitude
    attitude = _get_ci(
        analysis,
        [
            "Agent Attitude",
            "agent_attitude",
            "Agents Professionalism",
            "professionalism",
            "Agent Behavior",
            "agent_behavior",
            "Agent Tone",
            "agent_tone",
        ],
    )
    # fallbacks
    if category is None:
        # sometimes stored in disposition.score but that's really outcome; use if empty
        category = _get_ci(analysis.get("disposition", {}) if isinstance(analysis, dict) else {}, ["score"]) or None
    return (str(category) if category is not None else None, str(attitude) if attitude is not None else None)


def _lower_key_map(d: dict) -> dict:
    return {k.lower(): k for k in d.keys()} if isinstance(d, dict) else {}


def _get_nested_block(analysis: dict, candidates: list[str]) -> dict | None:
    if not isinstance(analysis, dict):
        return None
    lower_map = _lower_key_map(analysis)
    for k in candidates:
        real = lower_map.get(k.lower())
        if real is not None and isinstance(analysis.get(real), dict):
            return analysis.get(real)  # type: ignore
    return None


def _extract_structured_fields(analysis: Any) -> Dict[str, Any]:
    """Extract fields for details view from the analysis JSON.
    Returns a flat dict with normalized keys.
    """
    out: Dict[str, Any] = {
        "customer_sentiment": None,
        "call_categorization": None,
        "resolution_status": None,
        "main_subject": None,
        "services": None,
        "call_outcome": None,
        "agent_attitude": None,
        "agent_professionalism": None,
        "call_summary": None,
        "fcr": None,
        "aht": None,
        "talk_time_seconds": None,
        "hold_time_seconds": None,
        "after_call_work_seconds": None,
    }
    if not isinstance(analysis, dict):
        return out

    # Insights block
    insights = _get_nested_block(analysis, [
        "Call Generated Insights", "call_generated_insights", "generated_insights", "insights",
    ])
    if isinstance(insights, dict):
        out["customer_sentiment"] = _get_ci(insights, ["Customer Sentiment"])  # Positive/Neutral/Negative
        out["call_categorization"] = _get_ci(insights, ["Call Categorization", "Call Category", "category"])  # Inquiry/Issue/etc
        out["resolution_status"] = _get_ci(insights, ["Resolution Status"])  # resolved/escalated/pending
        out["main_subject"] = _get_ci(insights, ["Main Subject", "subject"])  # text
        out["services"] = _get_ci(insights, ["Services"])  # text/list
        out["call_outcome"] = _get_ci(insights, ["Call Outcome"])  # text
        out["agent_attitude"] = _get_ci(insights, ["Agent Attitude"])  # text
        out["agent_professionalism"] = _get_ci(insights, ["Agents Professionalism", "Agent Professionalism", "agent_professionalism", "professionalism"])  # text
        out["call_summary"] = _get_ci(insights, ["Call Summary"]) or analysis.get("summary")

    # Metrics block
    metrics = _get_nested_block(analysis, [
        "Customer Service Metrics", "customer_service_metrics", "metrics",
    ])

    # FCR
    if isinstance(metrics, dict):
        fcr = metrics.get(_lower_key_map(metrics).get("fcr"))
        if fcr is None and "FCR" in analysis:
            fcr = analysis.get("FCR")
        out["fcr"] = fcr
    else:
        out["fcr"] = analysis.get("FCR")

    # AHT
    aht = None
    if isinstance(metrics, dict):
        aht = metrics.get(_lower_key_map(metrics).get("aht"))
    if aht is None:
        # Prefer top-level Average Handling Time (AHT)
        aht = analysis.get("Average Handling Time (AHT)") or analysis.get("AHT")
    out["aht"] = aht

    # Talk/Hold/After-call seconds
    # Try in metrics then top-level using common variants
    def find_time(obj: dict, keys: list[str]):
        if not isinstance(obj, dict):
            return None
        lm = _lower_key_map(obj)
        for k in keys:
            real = lm.get(k.lower())
            if real is not None:
                return obj.get(real)
        return None

    for src in [metrics, analysis]:
        if out["talk_time_seconds"] is None:
            out["talk_time_seconds"] = find_time(src or {}, ["talk_time_seconds", "Talk time", "talk time", "talk_time"])  # type: ignore
        if out["hold_time_seconds"] is None:
            out["hold_time_seconds"] = find_time(src or {}, ["hold_time_seconds", "Hold time", "hold time", "hold_time"])  # type: ignore
        if out["after_call_work_seconds"] is None:
            out["after_call_work_seconds"] = find_time(src or {}, ["after_call_work_seconds", "After call work", "after_call_work"])  # type: ignore

    # Fallback: derive professionalism from attitude keywords if not present
    if not out.get("agent_professionalism"):
        att = str(out.get("agent_attitude") or "").lower()
        if att:
            if any(k in att for k in ["empathetic", "helpful", "attentive", "outstanding", "excellent", "very good", "highly"]):
                out["agent_professionalism"] = "Highly Professional"
            elif any(k in att for k in ["defensive", "rude", "angry", "poor", "unprofessional", "needs improvement", "improve"]):
                out["agent_professionalism"] = "Needs Improvement"
            else:
                out["agent_professionalism"] = "Professional"

    return out

@app.route('/upload-complete', methods=['POST'])
async def upload_complete_pipeline(
    files: List[UploadFile] = File(...),
) -> Dict[str, Any]:
    """Complete pipeline: Upload → Transcribe → Analyze → Index for search"""
    results: List[Dict[str, Any]] = []
    
    for uf in files:
        try:
            filename = uf.filename.replace(" ", "_")
            content = await uf.read()
            
            # Step 1: Upload audio to blob storage
            print(f"Processing {filename}: Step 1 - Uploading to blob storage...")
            azure_storage.upload_blob(content, filename, prefix=azure_storage.AUDIO_FOLDER)
            name_no_ext = filename.rsplit(".", 1)[0]
            
            # Step 2: Transcribe audio using Azure Speech services
            print(f"Processing {filename}: Step 2 - Transcribing with Azure Speech...")
            transcript = azure_transcription.transcribe_audio(filename)
            
            # Check if transcription failed
            if transcript.startswith("Error:") or transcript.startswith("Audio validation failed:"):
                print(f"Transcription failed for {filename}: {transcript}")
                results.append({
                    "file": filename,
                    "error": f"Transcription failed: {transcript}",
                    "search_indexed": False,
                })
                continue
                
            # Save successful transcription
            azure_storage.upload_transcription_to_blob(name_no_ext, transcript)
            print(f"Processing {filename}: Step 2 - Transcription completed successfully")
            
            # Step 3: Analyze transcript with GenAI using static system prompt
            print(f"Processing {filename}: Step 3 - Analyzing with GenAI...")
            analysis_raw = azure_oai.call_llm(SYSTEM_PROMPT_DEFAULT, transcript)
            analysis_json = _parse_json_maybe(analysis_raw)
            
            # Save analysis to both default and persona folders for compatibility
            azure_storage.upload_blob(
                json.dumps(analysis_json),
                f"default/{name_no_ext}.json",
                prefix=azure_storage.LLM_ANALYSIS_FOLDER,
            )
            azure_storage.upload_blob(
                json.dumps(analysis_json),
                f"persona/{name_no_ext}.json",
                prefix=azure_storage.LLM_ANALYSIS_FOLDER,
            )
            print(f"Processing {filename}: Step 3 - Analysis completed successfully")
            
            # Step 4: Update Azure AI Search index for chat functionality
            print(f"Processing {filename}: Step 4 - Indexing for search...")
            try:
                # Get current document count before indexing
                current_count = azure_search.get_index_document_count("marketing_sentiment_details")
                print(f"Current index document count: {current_count}")
                
                # Load the analysis JSON into the marketing_sentiment_details index
                message, success = azure_search.load_json_into_azure_search(
                    "marketing_sentiment_details", 
                    [analysis_json]
                )
                if not success:
                    print(f"Warning: Failed to index {name_no_ext} for search: {message}")
                    search_indexed = False
                else:
                    search_indexed = True
                    # Get new document count after indexing
                    new_count = azure_search.get_index_document_count("marketing_sentiment_details")
                    print(f"Indexing completed. New document count: {new_count}")
                    if new_count > current_count:
                        print(f"Successfully added {new_count - current_count} new document(s) to search index")
                    else:
                        print(f"Document count unchanged. Document may have been updated rather than added.")
                    
                    print(f"Processing {filename}: Step 4 - Search indexing completed successfully")
            except Exception as e:
                print(f"Warning: Search indexing failed for {name_no_ext}: {e}")
                search_indexed = False
            
            results.append({
                "file": filename,
                "transcription_blob": f"{azure_storage.TRANSCRIPTION_FOLDER}/{name_no_ext}.txt",
                "analysis_blob": f"{azure_storage.LLM_ANALYSIS_FOLDER}/persona/{name_no_ext}.json",
                "search_indexed": search_indexed,
            })
            
            print(f"Processing {filename}: All steps completed successfully")
            
        except Exception as e:
            # Log error but continue with other files
            error_msg = f"Error processing {uf.filename}: {str(e)}"
            print(error_msg)
            results.append({
                "file": uf.filename,
                "error": error_msg,
                "search_indexed": False,
            })
    
    return {"status": "ok", "processed": results}


@app.route('/upload', methods=['POST'])
async def upload_and_process(
    files: List[UploadFile] = File(...),
) -> Dict[str, Any]:
    """Legacy endpoint - now redirects to complete pipeline"""
    return await upload_complete_pipeline(files)


@app.route('/health', methods=['GET'])
def health() -> Dict[str, Any]:
    try:
        azure_storage.ensure_container_exists(azure_storage.DEFAULT_CONTAINER)
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.route('/calls', methods=['GET'])
def list_calls() -> List[Dict[str, Any]]:
    try:
        # Ensure container exists
        azure_storage.ensure_container_exists(azure_storage.DEFAULT_CONTAINER)

        container = azure_storage.blob_service_client.get_container_client(azure_storage.DEFAULT_CONTAINER)

        # Gather audio blobs from both the configured folder and anywhere in the container
        audio_exts = (".mp3", ".wav", ".m4a", ".mp4")
        audio_blobs = list(container.list_blobs(name_starts_with=f"{azure_storage.AUDIO_FOLDER}/"))
        if not audio_blobs:
            # Fallback: scan entire container for audio extensions
            audio_blobs = [b for b in container.list_blobs() if any(b.name.lower().endswith(ext) for ext in audio_exts)]

        entries: List[Dict[str, Any]] = []
        seen_call_ids: set[str] = set()
        for blob in audio_blobs:
            audio_path = blob.name
            audio_name = audio_path.split("/")[-1]
            call_id = audio_name.rsplit(".", 1)[0]
            if call_id in seen_call_ids:
                continue
            seen_call_ids.add(call_id)

            # Prefer persona analysis folder
            parsed, first_analysis_path = _persona_analysis_for_call(call_id)
            if not parsed:
                # Fallback: any analysis folder
                parsed, first_analysis_path = _first_analysis_for_call(call_id)
            category, attitude = _derive_category_and_attitude(parsed)
            # If still missing, try a second pass: look for JSON named exactly by audio base even if extension differs
            if (category is None or attitude is None) and not parsed:
                # attempt: if audio file has dashes or underscores variations, try normalized id
                norm_id = call_id.replace(" ", "_")
                if norm_id != call_id:
                    parsed, first_analysis_path = _persona_analysis_for_call(norm_id)
                    if not parsed:
                        parsed, first_analysis_path = _first_analysis_for_call(norm_id)
                    c2, a2 = _derive_category_and_attitude(parsed)
                    category = category or c2
                    attitude = attitude or a2
            created = getattr(blob, "creation_time", None) or getattr(blob, "last_modified", None)
            entries.append({
                "audio_name": audio_name,
                "call_id": call_id,
                "uploaded_at": created.isoformat() if isinstance(created, datetime) else None,
                "analysis": parsed,
                "call_category": category,
                "agent_attitude": attitude,
                "analysis_file": first_analysis_path,
            })

        # newest first
        entries.sort(key=lambda x: x.get("uploaded_at") or "", reverse=True)
        return entries
    except Exception:
        # Fallback to simpler listing to avoid 500
        try:
            audios = azure_storage.list_audios()
            return [{"audio_name": a, "call_id": a.rsplit(".", 1)[0], "uploaded_at": None, "analysis": None, "analysis_files": []} for a in audios]
        except Exception:
            return []


@app.route('/calls/<call_id>', methods=['GET'])
def get_call(call_id: str) -> Dict[str, Any]:
    transcript = azure_storage.read_transcription(f"{call_id}.txt")
    # Prefer persona analysis
    analysis, analysis_path = _persona_analysis_for_call(call_id)
    if not analysis:
        # Fallback to any analysis
        analysis, analysis_path = _first_analysis_for_call(call_id)
    # SAS URL for audio streaming
    audio_sas = None
    try:
        path = azure_storage.find_audio_blob_path_for_call_id(call_id)
        if path:
            audio_sas = azure_storage.get_blob_sas_url_for_path(path)
    except Exception:
        audio_sas = None
    structured = _extract_structured_fields(analysis)
    return {
        "call_id": call_id,
        "audio_url": audio_sas,
        "transcript": transcript,
        "analysis": analysis,
        "insights": structured,
    }


@app.route('/dashboard/summary', methods=['GET'])
def dashboard_summary() -> Dict[str, Any]:
    calls = list_calls()
    summaries: List[str] = []
    sentiment_scores: List[float] = []
    sentiment_labels: Dict[str, int] = {}
    dispositions: Dict[str, int] = {}
    categories: Dict[str, int] = {}
    resolution_status: Dict[str, int] = {}
    subjects: Dict[str, int] = {}
    services: Dict[str, int] = {}
    agent_professionalism: Dict[str, int] = {}
    resolved_count = 0
    aht_values: List[float] = []
    talk_values: List[float] = []
    hold_values: List[float] = []

    for c in calls:
        a = c.get("analysis") or {}
        if isinstance(a, dict) and a.get("summary"):
            summaries.append(a["summary"]) 
        # sentiment numeric (1-5)
        s = a.get("sentiment", {})
        if isinstance(s, dict):
            score = s.get("score")
            try:
                sentiment_scores.append(float(score))
            except Exception:
                pass
        # disposition counts
        disp = a.get("disposition") or a.get("Disposition")
        if isinstance(disp, dict):
            dscore = disp.get("score")
            if dscore:
                dispositions[str(dscore)] = dispositions.get(str(dscore), 0) + 1
        # resolved
        resolved = a.get("resolved")
        if isinstance(resolved, dict) and resolved.get("score") is True:
            resolved_count += 1

        # structured insights
        structured = _extract_structured_fields(a)
        if structured.get("customer_sentiment"):
            lbl = str(structured["customer_sentiment"]).strip()
            sentiment_labels[lbl] = sentiment_labels.get(lbl, 0) + 1
        if structured.get("call_categorization"):
            cat = str(structured["call_categorization"]).strip()
            categories[cat] = categories.get(cat, 0) + 1
        if structured.get("resolution_status"):
            rs = str(structured["resolution_status"]).strip()
            resolution_status[rs] = resolution_status.get(rs, 0) + 1
        if structured.get("main_subject"):
            subjects[str(structured["main_subject"]).strip()] = subjects.get(str(structured["main_subject"]).strip(), 0) + 1
        if structured.get("services"):
            # split on comma or semicolon into multiple services
            sv = structured["services"]
            if isinstance(sv, str):
                parts = [p.strip() for p in sv.replace(";", ",").split(",") if p.strip()]
                for p in parts:
                    services[p] = services.get(p, 0) + 1
            elif isinstance(sv, list):
                for p in sv:
                    services[str(p).strip()] = services.get(str(p).strip(), 0) + 1
        # Agent professionalism/attitude histogram
        if structured.get("agent_professionalism"):
            att = str(structured.get("agent_professionalism")).strip()
            if att:
                agent_professionalism[att] = agent_professionalism.get(att, 0) + 1
        # AHT and times
        aht = structured.get("aht")
        if isinstance(aht, dict):
            try:
                aht_values.append(float(aht.get("score")))
            except Exception:
                pass
        if structured.get("talk_time_seconds") is not None:
            try:
                talk_values.append(float(structured.get("talk_time_seconds")))
            except Exception:
                pass
        if structured.get("hold_time_seconds") is not None:
            try:
                hold_values.append(float(structured.get("hold_time_seconds")))
            except Exception:
                pass

    overall_insights = None
    if summaries:
        try:
            overall_insights = azure_oai.get_insights(summaries)
        except Exception:
            overall_insights = None

    total = len(calls)
    avg_sentiment = sum(sentiment_scores) / len(sentiment_scores) if sentiment_scores else None
    avg_aht = sum(aht_values) / len(aht_values) if aht_values else None
    avg_talk = sum(talk_values) / len(talk_values) if talk_values else None
    avg_hold = sum(hold_values) / len(hold_values) if hold_values else None
    return {
        "total_calls": total,
        "avg_sentiment": avg_sentiment,
        "sentiment_labels": sentiment_labels,
        "dispositions": dispositions,
        "categories": categories,
        "resolution_status": resolution_status,
        "subjects": subjects,
        "services": services,
        "agent_professionalism": agent_professionalism,
        "resolved_rate": (resolved_count / total) if total else None,
        "avg_aht_seconds": avg_aht,
        "avg_talk_seconds": avg_talk,
        "avg_hold_seconds": avg_hold,
        "overall_insights": overall_insights,
    }

@app.route('/insights', methods=['GET'])
def get_insights() -> Dict[str, Any]:
    """Get AI-generated comprehensive insights and summary from all call summaries"""
    try:
        calls = list_calls()
        summaries: List[str] = []
        
        # Collect all call summaries
        for c in calls:
            a = c.get("analysis") or {}
            if isinstance(a, dict) and a.get("summary"):
                summaries.append(a["summary"])
        
        overall_insights = None
        comprehensive_summary = None
        
        if summaries:
            try:
                # Get raw AI insights
                overall_insights = azure_oai.get_insights(summaries)
                
                # Generate comprehensive summary paragraph
                summary_prompt = """
                Based on the following call summaries, write a comprehensive, well-structured summary paragraph that covers:
                
                1. **Overall Call Volume and Trends**: Total number of calls and general patterns
                2. **Main Issues and Topics**: Most common problems or subjects discussed
                3. **Customer Satisfaction**: Overall sentiment and satisfaction levels
                4. **Agent Performance**: How well agents are handling calls
                5. **Key Insights**: Important observations and patterns
                6. **Recommendations**: Actionable suggestions for improvement
                
                Write this as a professional, easy-to-read paragraph with clear sections. Use bullet points or numbered lists where appropriate.
                Focus on providing actionable insights that management can use.
                """
                
                comprehensive_summary = azure_oai.call_llm(summary_prompt, "\n\n".join(summaries))
                
            except Exception as e:
                print(f"Error generating insights: {e}")
                overall_insights = None
                comprehensive_summary = None
        
        return {
            "status": "ok",
            "insights": overall_insights or "No insights available yet.",
            "comprehensive_summary": comprehensive_summary or "No summary available yet.",
            "total_calls_analyzed": len(summaries),
            "total_calls": len(calls),
            "summary_breakdown": {
                "total_calls": len(calls),
                "calls_with_summaries": len(summaries),
                "coverage_percentage": round((len(summaries) / len(calls) * 100), 1) if calls else 0
            }
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to generate insights: {str(e)}",
            "insights": None,
            "comprehensive_summary": None,
            "total_calls_analyzed": 0,
            "total_calls": 0
        }



@app.route('/chat', methods=['POST'])
def chat_with_data(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Chat with calls using Azure AI Search index 'marketing_sentiment_details' for retrieval, with long-chat handling.
    Body: { query: string, history?: [{role: 'user'|'ai', text: string}], top_k?: int }
    """
    query = (payload or {}).get("query", "").strip()
    if not query:
        return {"answer": "Please provide a query."}
    history = (payload or {}).get("history", []) or []
    top_k = int((payload or {}).get("top_k", 6))

    # Retrieve relevant docs from Azure Search
    try:
        results = azure_search.search_query("marketing_sentiment_details", query)
    except Exception:
        results = []

    # Ensure text list
    def _to_text_list(objs):
        if not objs:
            return []
        texts = []
        for o in objs:
            if isinstance(o, str):
                texts.append(o)
            elif isinstance(o, dict):
                for k in ["content", "text", "summary", "chunk", "body"]:
                    v = o.get(k)
                    if isinstance(v, str):
                        texts.append(v)
                        break
            else:
                try:
                    texts.append(str(o))
                except Exception:
                    pass
        return texts

    context_chunks = _to_text_list(results)
    provided_context = "\n\n".join(context_chunks[:top_k])

    # Optional: include persona prompt instructions if available
    persona_prompt_files = azure_storage.list_prompts() or []
    persona_context = ""
    for fname in persona_prompt_files:
        if fname.lower().startswith("persona"):
            persona_context = azure_storage.read_prompt(fname) or ""
            break

    system_prompt = (
        "You are a helpful assistant that analyzes call center conversations. "
        "Use ONLY the provided context and conversation history to answer the user's question. "
        "If the answer isn't clearly present, say you don't have enough information. "
        "Answer in clear, natural paragraphs that are easy to read and understand. "
        "Do NOT return JSON format or code blocks. Instead, provide insights in conversational language. "
        "When discussing call data, use friendly, professional language and organize information clearly. "
        "If you find multiple calls, summarize them in a readable way with bullet points or clear sections. "
        "Always be helpful and provide actionable insights when possible.\n\n"
        "Persona guidance (optional):\n" + (persona_context or "")
    )

    messages = [{"role": "system", "content": system_prompt}, {"role": "user", "content": f"Context Documents:\n{provided_context}"}]
    # append compacted recent history (last 6 turns)
    try:
        trimmed = history[-12:]
        for h in trimmed:
            role = h.get("role", "user")
            text = h.get("text", "")
            if text:
                messages.append({"role": "assistant" if role == "ai" else "user", "content": text})
    except Exception:
        pass
    messages.append({"role": "user", "content": f"User Question: {query}"})

    try:
        client = azure_oai.get_oai_client()
        completion = client.chat.completions.create(
            model=azure_oai.AZURE_OPENAI_DEPLOYMENT_NAME,
            messages=messages,
            temperature=0.2,
            top_p=1,
            max_tokens=1200,
        )
        answer = completion.choices[0].message.content or ""
    except Exception as e:
        answer = f"Unable to answer at the moment: {e}"

    return {"answer": answer}


@app.route('/diagnostics/audio/{filename}', methods=['GET'])
def diagnose_audio_file(filename: str) -> Dict[str, Any]:
    """Diagnose audio file issues for troubleshooting transcription problems."""
    try:
        # Get basic file info
        file_info = azure_storage.get_audio_file_info(filename)
        
        # Validate file format
        is_valid, validation_msg = azure_storage.validate_audio_file_format(filename)
        
        # Check if transcription exists
        transcription_exists = False
        transcription_content = None
        try:
            name_no_ext = filename.rsplit(".", 1)[0]
            transcription_content = azure_storage.read_transcription(f"{name_no_ext}.txt")
            transcription_exists = transcription_content is not None
        except Exception:
            pass
        
        # Check if analysis exists
        analysis_exists = False
        analysis_content = None
        try:
            name_no_ext = filename.rsplit(".", 1)[0]
            analysis_content = azure_storage.read_blob(f"persona/{name_no_ext}.json", prefix=azure_storage.LLM_ANALYSIS_FOLDER)
            analysis_exists = analysis_content is not None
        except Exception:
            pass
        
        return {
            "filename": filename,
            "file_info": file_info,
            "validation": {
                "is_valid": is_valid,
                "message": validation_msg
            },
            "transcription": {
                "exists": transcription_exists,
                "content_preview": transcription_content[:500] + "..." if transcription_content and len(transcription_content) > 500 else transcription_content
            },
            "analysis": {
                "exists": analysis_exists,
                "content_preview": analysis_content[:500] + "..." if analysis_content and len(analysis_content) > 500 else analysis_content
            },
            "recommendations": []
        }
        
    except Exception as e:
        return {
            "filename": filename,
            "error": str(e),
            "recommendations": [
                "Check if the file exists in blob storage",
                "Verify the file format is supported (.mp3, .wav, .m4a, .mp4, .aac, .ogg)",
                "Ensure the file is not corrupted or empty",
                "Check file size (should be between 1KB and 100MB)"
            ]
        }

@app.route('/diagnostics/transcription/{filename}', methods=['GET'])
def test_transcription(filename: str) -> Dict[str, Any]:
    """Test transcription for a specific audio file to identify issues."""
    try:
        # Validate file first
        is_valid, validation_msg = azure_storage.validate_audio_file_format(filename)
        if not is_valid:
            return {
                "filename": filename,
                "status": "validation_failed",
                "error": validation_msg,
                "recommendations": [
                    "Fix the file format issues before attempting transcription",
                    "Ensure the audio file is not corrupted",
                    "Check if the file size is within acceptable limits"
                ]
            }
        
        # Try transcription
        print(f"Testing transcription for {filename}...")
        transcript = azure_transcription.transcribe_audio(filename)
        
        if transcript.startswith("Error:") or transcript.startswith("Audio validation failed:"):
            return {
                "filename": filename,
                "status": "transcription_failed",
                "error": transcript,
                "recommendations": [
                    "Check Azure Speech service configuration",
                    "Verify audio file format compatibility",
                    "Ensure the audio contains speech content",
                    "Check network connectivity to Azure services"
                ]
            }
        
        # Success
        return {
            "filename": filename,
            "status": "transcription_successful",
            "transcript_preview": transcript[:500] + "..." if len(transcript) > 500 else transcript,
            "transcript_length": len(transcript),
            "recommendations": [
                "Transcription completed successfully",
                "File is ready for analysis and indexing"
            ]
        }
        
    except Exception as e:
        return {
            "filename": filename,
            "status": "error",
            "error": str(e),
            "recommendations": [
                "Check the server logs for detailed error information",
                "Verify Azure service credentials and configuration",
                "Ensure the audio file is accessible"
            ]
        }

@app.route('/diagnostics/search/{index_name}', methods=['GET'])
def diagnose_search_index(index_name: str) -> Dict[str, Any]:
    """Diagnose Azure Search index status and document count."""
    try:
        # Check if index exists
        index_exists = azure_search.index_exists(index_name)
        
        if not index_exists:
            return {
                "index_name": index_name,
                "status": "not_found",
                "message": f"Index '{index_name}' does not exist",
                "recommendations": [
                    "Upload some audio files to create the index",
                    "Check if the index name is correct",
                    "Verify Azure Search service configuration"
                ]
            }
        
        # Get document count
        doc_count = azure_search.get_index_document_count(index_name)
        
        # Get sample documents
        sample_docs = azure_search.list_index_documents(index_name, top=3)
        
        # Get index details
        try:
            index_client = azure_search.get_search_index_client()
            index_details = index_client.get_index(index_name)
            field_count = len(index_details.fields) if index_details.fields else 0
        except Exception as e:
            field_count = "unknown"
            index_details = None
        
        return {
            "index_name": index_name,
            "status": "active",
            "document_count": doc_count,
            "field_count": field_count,
            "sample_documents": sample_docs,
            "index_details": {
                "name": index_details.name if index_details else None,
                "field_names": [f.name for f in index_details.fields] if index_details and index_details.fields else []
            },
            "recommendations": [
                f"Index contains {doc_count} documents",
                "Use /diagnostics/audio/{filename} to check individual files",
                "Use /diagnostics/transcription/{filename} to test transcription"
            ]
        }
        
    except Exception as e:
        return {
            "index_name": index_name,
            "status": "error",
            "error": str(e),
            "recommendations": [
                "Check Azure Search service configuration",
                "Verify API keys and endpoints",
                "Check network connectivity to Azure Search"
            ]
        }

@app.route('/reindex-all-calls', methods=['POST'])
def reindex_all_calls() -> Dict[str, Any]:
    """Re-index all existing calls into the Azure Search index for chat functionality."""
    try:
        print("Starting re-indexing of all existing calls...")
        
        # Get all calls from the container
        calls = list_calls()
        if not calls:
            return {
                "status": "no_calls",
                "message": "No calls found to re-index",
                "indexed_count": 0,
                "total_calls": 0
            }
        
        # Get current index document count
        current_count = azure_search.get_index_document_count("marketing_sentiment_details")
        print(f"Current index document count: {current_count}")
        
        # Collect all analysis JSONs
        analysis_docs = []
        indexed_count = 0
        failed_count = 0
        
        for call in calls:
            try:
                call_id = call.get("call_id")
                analysis = call.get("analysis")
                
                if not analysis or not isinstance(analysis, dict):
                    print(f"Skipping {call_id}: No analysis data")
                    continue
                
                # Check if this document is already in the index
                # We'll use the call_id as a unique identifier
                analysis_docs.append({
                    "call_id": call_id,
                    "analysis": analysis
                })
                
            except Exception as e:
                print(f"Error processing call {call.get('call_id', 'unknown')}: {e}")
                failed_count += 1
        
        if not analysis_docs:
            return {
                "status": "no_analyses",
                "message": "No analysis documents found to index",
                "indexed_count": 0,
                "total_calls": len(calls)
            }
        
        print(f"Found {len(analysis_docs)} analysis documents to index")
        
        # Clear existing index and recreate with all documents
        try:
            # Delete existing index
            print("Deleting existing index to recreate with all documents...")
            azure_search.get_search_index_client().delete_index("marketing_sentiment_details")
            
            # Wait for deletion to complete
            import time
            time.sleep(3)
            
            # Create new index with first document as template
            if analysis_docs:
                first_doc = analysis_docs[0]["analysis"]
                message, success = azure_search.create_or_update_index("marketing_sentiment_details", first_doc)
                if not success:
                    return {
                        "status": "index_creation_failed",
                        "message": f"Failed to create index: {message}",
                        "indexed_count": 0,
                        "total_calls": len(calls)
                    }
                print("Index created successfully")
            
            # Index all documents
            print("Indexing all analysis documents...")
            message, success = azure_search.load_json_into_azure_search(
                "marketing_sentiment_details", 
                [doc["analysis"] for doc in analysis_docs]
            )
            
            if success:
                # Get new document count
                new_count = azure_search.get_index_document_count("marketing_sentiment_details")
                indexed_count = new_count
                
                return {
                    "status": "success",
                    "message": f"Successfully re-indexed {indexed_count} documents",
                    "indexed_count": indexed_count,
                    "total_calls": len(calls),
                    "previous_count": current_count,
                    "new_count": new_count
                }
            else:
                return {
                    "status": "indexing_failed",
                    "message": f"Failed to index documents: {message}",
                    "indexed_count": 0,
                    "total_calls": len(calls)
                }
                
        except Exception as e:
            return {
                "status": "error",
                "message": f"Error during re-indexing: {str(e)}",
                "indexed_count": 0,
                "total_calls": len(calls)
            }
            
    except Exception as e:
        return {
            "status": "error",
            "message": f"Unexpected error: {str(e)}",
            "indexed_count": 0,
            "total_calls": 0
        }



