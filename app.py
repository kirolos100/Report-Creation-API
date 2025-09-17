from typing import List, Dict, Any
import json
import os
from datetime import datetime
from services import azure_storage, azure_transcription, azure_oai, azure_search
from transcription_revision_service import revision_service
from flask import Flask, request, jsonify
import json
import requests
from bs4 import BeautifulSoup
from openai import AzureOpenAI
from flasgger import Swagger, swag_from
from flask_swagger_ui import get_swaggerui_blueprint
from flask_cors import CORS  # Import CORS
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

app = Flask(__name__)

# Configure CORS to allow your frontend origin
CORS(
    app,
    resources={r"/*": {"origins": [
        "https://green-smoke-05633cb03-preview.westeurope.2.azurestaticapps.net",
        "http://localhost:3000",
        "http://localhost:5173",
    ]}},
    methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH", "HEAD"],
    allow_headers=[
        "Content-Type",
        "Authorization",
        "Accept",
        "Cache-Control",       # <--- ensure Cache-Control is explicitly allowed
        "X-Requested-With",
        "Origin",
        "Accept-Language",
    ],
    supports_credentials=True,
    expose_headers=["Content-Disposition"],
)
# Fallback: ensure CORS headers are always present for allowed origins (incl. on errors)
@app.after_request
def add_cors_headers(response):
    try:
        origin = request.headers.get("Origin", "")
        allowed_origins = {
            "https://green-smoke-05633cb03-preview.westeurope.2.azurestaticapps.net",
            "http://localhost:3000",
            "http://localhost:5173",
        }
        if origin in allowed_origins:
            response.headers["Access-Control-Allow-Origin"] = origin
            response.headers["Vary"] = ", ".join(filter(None, [response.headers.get("Vary"), "Origin"]))
            response.headers["Access-Control-Allow-Credentials"] = "true"
            response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS, PATCH, HEAD"
            # Echo requested headers if present; otherwise allow common ones
            req_headers = request.headers.get("Access-Control-Request-Headers")
            if req_headers:
                response.headers["Access-Control-Allow-Headers"] = req_headers
            else:
                response.headers["Access-Control-Allow-Headers"] = \
                    "Content-Type, Authorization, Accept, Cache-Control, X-Requested-With, Origin, Accept-Language"
            # Expose headers used by downloads
            response.headers["Access-Control-Expose-Headers"] = ", ".join(
                sorted(set([*(response.headers.get("Access-Control-Expose-Headers", "").split(",") or []), "Content-Disposition"]))
            ).strip(", ")
    except Exception:
        pass
    return response
# Health check endpoint
@app.route('/', methods=['GET'])
def read_root():
    # Health check without forcing cache invalidation
    try:
        # Prefer Mongo for dashboard info; compute and upsert if missing
        mongo_doc = mongo_get_dashboard_summary()
        if mongo_doc is None:
            dashboard_data = calculate_dashboard_summary()
            try:
                mongo_upsert_dashboard_summary(dashboard_data)
            except Exception:
                pass
            mongo_doc = dashboard_data
        return {
            "status": "healthy", 
            "message": "API is running",
            "dashboard_summary_cached": True,
            "total_calls": mongo_doc.get("total_calls", 0),
            "calls_with_analysis": mongo_doc.get("calls_with_analysis", 0),
            "cache_version": _get_cache_version(),
            "last_change": datetime.fromtimestamp(_LAST_CHANGE_TIMESTAMP).isoformat() if _LAST_CHANGE_TIMESTAMP > 0 else "Never"
        }
    except Exception as e:
        print(f"Error calculating dashboard summary in health check: {e}")
        return {
            "status": "healthy", 
            "message": "API is running",
            "dashboard_summary_cached": False,
            "error": str(e)
        }



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
    "Call Categorization": "<Inquiry|Product/Service|Complaint|Other>",
    "Resolution Status": "<resolved|escalated|pending|other>",
    "Main Subject": "<short subject>",
    "Main Topic": "<Installation and Setup Issues|Repair and Maintenance Concerns|Warranty and Replacement Queries|Product Availability and Purchase Inquiries|Customer Service and Communication Issues>",
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
    "Talk time": <integer seconds (never being 0 seconds)>,
    "Hold time": <integer seconds (never being 0 seconds)>
  }
}
````

**Mandatory parsing & calculation rules (follow exactly):**
Don't change any key names or structure. You must return all keys shown above.

1. **Timestamps:** Parse timestamps in `HH:MM:SS`, `MM:SS`, or plain seconds; convert all to integer seconds before calculations. If timestamps are relative offsets, assume they are measured from call start — state this in any explanation that relies on it.

2. **Talk time (`Customer Service Metrics -> "Talk time"`):**

   * If utterances include start and end timestamps, compute each utterance duration as `end - start` and sum durations for Agent + Customer utterances.
   * If only start timestamps or offsets are available, infer utterance duration conservatively (document assumptions in the relevant `"explanation"` field).
   * Output MUST be an integer number of seconds (never 0).

3. **Hold time (`Customer Service Metrics -> "Hold time"`):**
  Never being 0 and should being calculated as:
   * Primary detection method (preferred): Detect explicit agent "please wait" utterances in the transcript text (Arabic and English). For Arabic transcripts, detect common agent waiting phrases or words and variants such as (but not limited to):
     "لحظة", "لحظات", "انتظر", "استنى", "استنّي", "خلي حضرتك", "هنرجع لك بعد شوي", "اسيبك على الانتظار", "معايا لحظة", "خلي حضرتك معايا لحظة", "معاك لحظة", "من فضلك انتظر", "هاخد منك لحظة", and obvious morphological variants or common colloquial spellings.
     When an agent utterance contains such a phrase or one word and indicates an intended hold, treat the hold start as that utterance's timestamp. Treat the hold end as the timestamp when the agent next resumes speaking (i.e., next agent utterance start time) or explicitly announces the end of hold. Compute hold duration as hold_end_timestamp - hold_start_timestamp in integer seconds.
     If the agent issues a "please wait" phrase and there is no later agent timestamp in the transcript to mark resumption, you must not output 0 for Hold time. Instead:

     * If a later customer utterance exists, conservatively treat the earlier of (a) the next customer utterance start time or (b) a minimum conservative default hold duration of 5 seconds after the "please wait" timestamp — whichever yields the larger hold duration — and document this choice in the "explanation" field.
     * If the transcript has no subsequent timestamps at all, estimate a conservative default hold duration of 5 seconds, and explain the assumption.
* When the Agent says words such as "لحظات", "طيب لحظات", or "لحظة", you must detect and calculate the hold time until the Agent returns and resumes the conversation with the Customer. (Make sure to search across all Agent speech segments for these keywords.) so the hold time shouldn't never calculated as 0 
   * If explicit markers are absent, infer hold from silence gaps between consecutive utterances where `gap >= 3 seconds` (gap = next_utterance_start − previous_utterance_end). Sum these inferred hold durations.
   * Output MUST be an integer number of seconds (never 0).
 * if you detects one second or more time hold you should write it in the json in the Hold time field that number of seconds and if you detect speaking time less than the total audio time so we have a hold time that was not calculated 
   * Prefer Arabic "please wait" detection and explicit markers over inferred silence. Always compute hold durations from timestamps and output as integer seconds (never 0). If exact timestamps are insufficient, estimate conservatively and explain assumptions concisely.
   * Important: Prefer Arabic "please wait" detection and explicit markers over silence inference. Always compute hold durations from timestamps and output integer seconds. Never output 0 for hold time if a "please wait" utterance is present — if timestamps are missing or incomplete for the hold, estimate conservatively and explain assumptions in the relevant "explanation" fields.
Output MUST be an integer number of seconds (never 0).
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
* Don't change any key names or structure. You must return all keys shown in the required Json.

**Example behavior (do not output this example in your response):**

* If the transcript includes explicit utterance start/end times, compute talk and hold precisely.
* If the transcript contains `[hold] 00:01:23 - 00:01:41`, add 18 seconds to Hold time.
* If gaps of 5–12 seconds exist and no hold markers are present, treat gaps ≥ 3 seconds as inferred hold.

You are responsible for correctly calculating and returning Talk time and Hold time (in seconds) and for producing the exact JSON structure above every time. If any part of the transcript is ambiguous, estimate conservatively, document the assumption in the appropriate `"explanation"` fields, and continue — but do not modify the JSON schema.
Don't change any key names or structure. You must return all keys shown above.



"""
)




# Global Mongo client for connection reuse
_mongo_client = None

def _get_mongo_client() -> MongoClient | None:
    """Create or reuse a Mongo client with optimized connection settings.
    Uses env var MONGO_URI; falls back to provided Cosmos Mongo connection string.
    """
    global _mongo_client
    
    if _mongo_client is not None:
        try:
            # Test if connection is still alive
            _mongo_client.admin.command('ping')
            return _mongo_client
        except Exception:
            # Connection is dead, recreate
            _mongo_client = None
    
    try:
        uri = os.getenv("MONGO_URI") or (
            "mongodb://elaraby:Nobq634p5m7vWAHj7OdMozizEilPLCmQbUhC9ZSiCyO7R8utvS3YuLOQp53eddGa3chb9Mrc8W9XACDbANN8og==@"
            "elaraby.mongo.cosmos.azure.com:10255/?ssl=true&retrywrites=false&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@elaraby@"
        )
        
        # Optimized connection settings for better performance
        _mongo_client = MongoClient(
            uri,
            maxPoolSize=10,  # Maximum number of connections in the pool
            minPoolSize=2,   # Minimum number of connections in the pool
            maxIdleTimeMS=30000,  # Close connections after 30 seconds of inactivity
            connectTimeoutMS=10000,  # 10 second connection timeout
            serverSelectionTimeoutMS=5000,  # 5 second server selection timeout
            socketTimeoutMS=20000,  # 20 second socket timeout
            retryWrites=False,  # Disable retry writes for Cosmos DB
            retryReads=False,   # Disable retry reads for Cosmos DB
        )
        
        # Test the connection
        _mongo_client.admin.command('ping')
        print("MongoDB connection established successfully")
        return _mongo_client
        
    except Exception as e:
        print(f"Mongo client init failed: {e}")
        _mongo_client = None
        return None


def _get_dashboard_collection(client: MongoClient | None) -> Collection | None:
    """Return the collection used to store dashboard summaries."""
    if client is None:
        return None
    try:
        db_name = os.getenv("MONGO_DB", "elaraby")
        coll_name = os.getenv("MONGO_DASHBOARD_COLLECTION", "dashboard_summaries")
        return client[db_name][coll_name]
    except Exception as e:
        print(f"Mongo get collection failed: {e}")
        return None


def mongo_upsert_dashboard_summary(summary: Dict[str, Any]) -> bool:
    """Upsert the latest dashboard summary into Mongo for instant reads."""
    try:
        client = _get_mongo_client()
        coll = _get_dashboard_collection(client)
        if coll is None:
            return False
        doc = dict(summary or {})
        doc["_id"] = "dashboard_summary_latest"
        doc["updated_at"] = datetime.utcnow()
        coll.replace_one({"_id": doc["_id"]}, doc, upsert=True)
        return True
    except PyMongoError as e:
        print(f"Mongo upsert error: {e}")
        return False
    except Exception as e:
        print(f"Mongo upsert unexpected error: {e}")
        return False


def mongo_get_dashboard_summary() -> Dict[str, Any] | None:
    """Fetch the latest dashboard summary from Mongo if present."""
    try:
        client = _get_mongo_client()
        coll = _get_dashboard_collection(client)
        if coll is None:
            return None
        doc = coll.find_one({"_id": "dashboard_summary_latest"})
        if not doc:
            return None
        # Remove internal fields
        doc.pop("_id", None)
        return doc
    except PyMongoError as e:
        print(f"Mongo get error: {e}")
        return None
    except Exception as e:
        print(f"Mongo get unexpected error: {e}")
        return None

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
# --------------------------
# Smart event-driven cache system
# --------------------------
_CACHE: Dict[str, Dict[str, Any]] = {}
_CACHE_VERSION: str = "1.0"  # Global cache version
_LAST_CHANGE_TIMESTAMP: float = 0  # Track when data last changed

def _get_cache_version() -> str:
    """Get current cache version based on last change timestamp."""
    return f"{_CACHE_VERSION}_{_LAST_CHANGE_TIMESTAMP}"

def _invalidate_cache() -> None:
    """Invalidate all caches by updating the change timestamp."""
    global _LAST_CHANGE_TIMESTAMP
    _LAST_CHANGE_TIMESTAMP = datetime.utcnow().timestamp()
    print(f"Cache invalidated at {datetime.utcnow().isoformat()}")

def _cache_get(key: str, ttl_seconds: int = 86400) -> Any | None:  # Default 24 hours
    """Get cached data, but check if cache version is still valid."""
    entry = _CACHE.get(key)
    if not entry:
        return None
    
    # Check if cache version is still current
    cached_version = entry.get("version")
    current_version = _get_cache_version()
    
    if cached_version != current_version:
        print(f"Cache version mismatch for {key}: cached={cached_version}, current={current_version}")
        _CACHE.pop(key, None)
        return None
    
    # Check TTL (but with very long default)
    ts = entry.get("ts")
    if not isinstance(ts, float):
        return None
    if (datetime.utcnow().timestamp() - ts) > ttl_seconds:
        _CACHE.pop(key, None)
        return None
    
    return entry.get("data")


def _check_blob_changes() -> bool:
    """Check if blob storage has changed by comparing file counts and timestamps."""
    try:
        container = azure_storage.blob_service_client.get_container_client(azure_storage.DEFAULT_CONTAINER)
        
        # Get current audio files
        audio_exts = (".mp3", ".wav", ".m4a", ".mp4")
        audio_blobs = list(container.list_blobs(name_starts_with=f"{azure_storage.AUDIO_FOLDER}/"))
        if not audio_blobs:
            audio_blobs = [b for b in container.list_blobs() if any(b.name.lower().endswith(ext) for ext in audio_exts)]
        
        # Create a signature of current state
        current_signature = {
            "count": len(audio_blobs),
            "files": sorted([b.name for b in audio_blobs]),
            "last_modified": max([b.last_modified.timestamp() for b in audio_blobs]) if audio_blobs else 0
        }
        
        # Check against cached signature
        cached_signature = _CACHE.get("_blob_signature")
        if cached_signature is None:
            # First time, cache the signature
            _CACHE["_blob_signature"] = {"data": current_signature, "ts": datetime.utcnow().timestamp(), "version": _get_cache_version()}
            return False
        
        # Compare signatures
        if cached_signature.get("data") != current_signature:
            print(f"Blob storage changed: {cached_signature.get('data')} -> {current_signature}")
            _invalidate_cache()
            _CACHE["_blob_signature"] = {"data": current_signature, "ts": datetime.utcnow().timestamp(), "version": _get_cache_version()}
            return True
        
        return False
        
    except Exception as e:
        print(f"Error checking blob changes: {e}")
        return False

def _cache_set(key: str, value: Any) -> None:
    """Set cached data with current version."""
    _CACHE[key] = {
        "data": value, 
        "ts": datetime.utcnow().timestamp(),
        "version": _get_cache_version()
    }


def save_dashboard_summary_to_blob(dashboard_data: Dict[str, Any]) -> bool:
    """Save dashboard summary data to blob storage as JSON file."""
    try:
        # Add timestamp to the data
        dashboard_data_with_timestamp = {
            **dashboard_data,
            "cached_at": datetime.utcnow().isoformat(),
            "cache_version": "1.0"
        }
        
        # Convert to JSON string
        json_data = json.dumps(dashboard_data_with_timestamp, indent=2)
        
        # Upload to blob storage
        azure_storage.upload_blob(
            json_data.encode('utf-8'),
            "dashboard_summary.json",
            prefix="cache",
        )
        
        print("Dashboard summary saved to blob storage successfully")
        return True
    except Exception as e:
        print(f"Error saving dashboard summary to blob storage: {e}")
        return False


def load_dashboard_summary_from_blob() -> Dict[str, Any] | None:
    """Load dashboard summary data from blob storage."""
    try:
        # Try to read from blob storage
        json_content = azure_storage.read_blob(
            "dashboard_summary.json",
            prefix="cache",
        )
        
        if json_content:
            dashboard_data = json.loads(json_content)
            print("Dashboard summary loaded from blob storage successfully")
            return dashboard_data
        else:
            print("No dashboard summary found in blob storage")
            return None
            
    except Exception as e:
        print(f"Error loading dashboard summary from blob storage: {e}")
        return None


def calculate_dashboard_summary() -> Dict[str, Any]:
    """Calculate dashboard summary data without caching."""
    calls = list_calls()
    summaries: List[str] = []
    sentiment_scores: List[float] = []
    sentiment_labels: Dict[str, int] = {}
    dispositions: Dict[str, int] = {}
    categories: Dict[str, int] = {}
    resolution_status: Dict[str, int] = {}
    subjects: Dict[str, int] = {}
    topics: Dict[str, int] = {}
    services: Dict[str, int] = {}
    agent_professionalism: Dict[str, int] = {}
    resolved_count = 0
    aht_values: List[float] = []
    talk_values: List[float] = []
    hold_values: List[float] = []
    calls_with_analysis = 0

    for c in calls:
        a = c.get("analysis") or {}
        if isinstance(a, dict) and a.get("summary"):
            summaries.append(a["summary"]) 
            calls_with_analysis += 1
            
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
        if structured.get("main_topic"):
            topic = str(structured["main_topic"]).strip()
            topics[topic] = topics.get(topic, 0) + 1
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
    result = {
        "total_calls": total,
        "calls_with_analysis": calls_with_analysis,
        "avg_sentiment": avg_sentiment,
        "sentiment_labels": sentiment_labels,
        "dispositions": dispositions,
        "categories": categories,
        "resolution_status": resolution_status,
        "subjects": subjects,
        "topics": topics,
        "services": services,
        "agent_professionalism": agent_professionalism,
        "resolved_rate": (resolved_count / total) if total else None,
        "avg_aht_seconds": avg_aht,
        "avg_talk_seconds": avg_talk,
        "avg_hold_seconds": avg_hold,
        "overall_insights": overall_insights,
    }
    return result


def clear_calls_cache() -> None:
    """Clear all calls-related cache entries using smart invalidation."""
    try:
        # Use smart invalidation instead of manual clearing
        _invalidate_cache()
        print("Calls cache invalidated using smart versioning")
    except Exception as e:
        print(f"Error clearing calls cache: {e}")


def refresh_calls_and_dashboard() -> Dict[str, Any]:
    """Refresh calls list and dashboard summary after deletions."""
    try:
        print("Refreshing calls list and dashboard summary...")
        
        # Clear calls cache to force fresh scan
        clear_calls_cache()
        
        # Calculate fresh dashboard summary
        dashboard_data = calculate_dashboard_summary()
        
        # Save updated dashboard to Mongo and Blob storage
        try:
            mongo_upsert_dashboard_summary(dashboard_data)
        except Exception:
            pass
        save_dashboard_summary_to_blob(dashboard_data)
        
        return {
            "status": "success",
            "message": "Calls list and dashboard summary refreshed successfully",
            "total_calls": dashboard_data.get("total_calls", 0),
            "refreshed_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error refreshing calls and dashboard: {str(e)}"
        }



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
        "main_topic": None,
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
        out["main_topic"] = _get_ci(insights, ["Main Topic", "main_topic"])  # Installation and Setup Issues|Repair and Maintenance Concerns|etc
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

@app.route('/upload-complete', methods=['POST', 'OPTIONS'])
def upload_complete_pipeline() -> Dict[str, Any]:
    """Complete pipeline: Upload → Transcribe → Analyze → Index for search"""
    
    # Handle OPTIONS preflight request
    if request.method == 'OPTIONS':
        return {"status": "ok"}
    
    results: List[Dict[str, Any]] = []
    
    # Get files from Flask request
    if 'files' not in request.files:
        return {"status": "error", "message": "No files provided", "processed": []}
    
    files = request.files.getlist('files')
    
    for uf in files:
        try:
            filename = uf.filename.replace(" ", "_")
            content = uf.read()
            
            # Step 1: Upload audio to blob storage
            print(f"Processing {filename}: Step 1 - Uploading to blob storage...")
            azure_storage.upload_blob(content, filename, prefix=azure_storage.AUDIO_FOLDER)
            name_no_ext = filename.rsplit(".", 1)[0]

            # Provisional: immediately upsert a fresh dashboard summary so UI can read updated counts without waiting
            try:
                provisional_summary = calculate_dashboard_summary()
                mongo_upsert_dashboard_summary(provisional_summary)
            except Exception as e:
                print(f"Warning: provisional dashboard upsert failed: {e}")
            
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
            
            # Step 2.5: Generate revised transcriptions (Arabic and English) in background
            print(f"Processing {filename}: Step 2.5 - Generating revised transcriptions...")
            try:
                # This will be done after analysis to have context
                pass  # We'll do this after step 3 to have analysis context
            except Exception as e:
                print(f"Note: Revised transcription generation will be done after analysis: {e}")
            
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
            
            # Step 3.5: Generate revised transcriptions with analysis context
            print(f"Processing {filename}: Step 3.5 - Generating revised transcriptions...")
            try:
                # Generate both Arabic and English revised transcriptions
                arabic_success, english_success, revision_message = revision_service.process_single_transcription(
                    name_no_ext, force_regenerate=False
                )
                
                if arabic_success and english_success:
                    print(f"✅ Both revised transcriptions created successfully for {filename}")
                elif arabic_success:
                    print(f"✅ Arabic revision created, English revision failed for {filename}")
                elif english_success:
                    print(f"✅ English revision created, Arabic revision failed for {filename}")
                else:
                    print(f"⚠️ Both revised transcriptions failed for {filename}: {revision_message}")
                    
            except Exception as e:
                print(f"Warning: Error generating revised transcriptions for {filename}: {e}")
                # Continue processing even if revision generation fails
            
            # Step 4: Update Azure AI Search index for chat functionality
            print(f"Processing {filename}: Step 4 - Indexing for search...")
            try:
                # Ensure index exists and is properly configured
                index_name = "marketing_sentiment_details"
                
                # Check if index exists, create if needed
                if not azure_search.index_exists(index_name):
                    print(f"Index '{index_name}' doesn't exist. Creating with sample document...")
                    create_message, create_success = azure_search.create_or_update_index(index_name, analysis_json)
                    if not create_success:
                        print(f"Failed to create index: {create_message}")
                        search_indexed = False
                        # Skip to the next step since index creation failed
                        raise Exception(f"Index creation failed: {create_message}")
                    print(f"Index created successfully: {create_message}")
                
                # Get current document count before indexing
                current_count = azure_search.get_index_document_count(index_name)
                print(f"Current index document count: {current_count}")
                
                # Prepare the analysis JSON for indexing
                analysis_payload = dict(analysis_json) if isinstance(analysis_json, dict) else {"raw": analysis_json}
                analysis_payload.setdefault("call_id", name_no_ext)
                analysis_payload.setdefault("id", name_no_ext)
                
                # Load the analysis JSON into the index using optimized method
                message, success, indexed_doc_ids = azure_search.load_json_into_azure_search_optimized(
                    index_name, [analysis_payload], wait_for_completion=True
                )
                
                if not success:
                    print(f"Warning: Failed to index {name_no_ext} for search: {message}")
                    search_indexed = False
                else:
                    print(f"✅ Document indexing completed: {message}")
                    search_indexed = True
                    
                    # Get final document count for logging
                    final_count = azure_search.get_index_document_count(index_name)
                    print(f"Index now contains {final_count} total documents")
                    
                    # Log the change in document count
                    if final_count > current_count:
                        print(f"📈 Added {final_count - current_count} new document(s) to index")
                    elif final_count == current_count and current_count > 0:
                        print(f"🔄 Document updated in existing index (count unchanged: {final_count})")
                    else:
                        print(f"ℹ️ Index document count: {final_count}")
                    
                    # Verify specific document was indexed
                    if name_no_ext in indexed_doc_ids:
                        print(f"✅ Document {name_no_ext} confirmed in search index")
                    else:
                        print(f"⚠️ Document {name_no_ext} may still be processing in background")
                    
                    print(f"Processing {filename}: Step 4 - Search indexing completed successfully")
                        
            except Exception as e:
                print(f"Error: Search indexing failed for {name_no_ext}: {e}")
                import traceback
                traceback.print_exc()
                search_indexed = False
            
            # Final index verification
            final_index_count = azure_search.get_index_document_count("marketing_sentiment_details") if search_indexed else 0
            
            results.append({
                "file": filename,
                "transcription_blob": f"{azure_storage.TRANSCRIPTION_FOLDER}/{name_no_ext}.txt",
                "revised_arabic_blob": f"{azure_storage.REVISED_ARABIC_FOLDER}/{name_no_ext}.txt",
                "revised_english_blob": f"{azure_storage.REVISED_ENGLISH_FOLDER}/{name_no_ext}.txt",
                "analysis_blob": f"{azure_storage.LLM_ANALYSIS_FOLDER}/persona/{name_no_ext}.json",
                "search_indexed": search_indexed,
                "index_document_count": final_index_count,
                "call_id": name_no_ext,
            })
            
            status_msg = "successfully" if search_indexed else "with indexing issues"
            print(f"Processing {filename}: All steps completed {status_msg} (Index count: {final_index_count})")
            
        except Exception as e:
            # Log error but continue with other files
            error_msg = f"Error processing {uf.filename}: {str(e)}"
            print(error_msg)
            results.append({
                "file": uf.filename,
                "error": error_msg,
                "search_indexed": False,
            })
    
    # Update dashboard summary and invalidate cache after processing all files
    try:
        print("Updating dashboard summary and invalidating cache after file processing...")
        
        # Invalidate all caches immediately when files are processed
        _invalidate_cache()
        
        # Calculate fresh dashboard summary
        dashboard_data = calculate_dashboard_summary()
        # Persist to Mongo for instant reads
        try:
            mongo_ok = mongo_upsert_dashboard_summary(dashboard_data)
            print(f"Mongo upsert dashboard summary: {mongo_ok}")
        except Exception as e:
            print(f"Warning: Mongo upsert failed: {e}")
        # Persist to Blob as secondary store
        save_dashboard_summary_to_blob(dashboard_data)
        
        print("Dashboard summary updated and cache invalidated successfully")
    except Exception as e:
        print(f"Error updating dashboard summary: {e}")
    
    # Final index status summary
    try:
        final_index_count = azure_search.get_index_document_count("marketing_sentiment_details")
        successfully_indexed = sum(1 for r in results if r.get("search_indexed", False))
        total_processed = len(results)
        
        print(f"\n=== UPLOAD SUMMARY ===")
        print(f"Files processed: {total_processed}")
        print(f"Successfully indexed: {successfully_indexed}")
        print(f"Final index document count: {final_index_count}")
        print(f"Index status: {'✅ Healthy' if final_index_count > 0 else '❌ Empty or Issues'}")
        
        return {
            "status": "ok", 
            "processed": results,
            "summary": {
                "total_processed": total_processed,
                "successfully_indexed": successfully_indexed,
                "final_index_count": final_index_count,
                "index_healthy": final_index_count > 0
            }
        }
    except Exception as e:
        print(f"Error generating upload summary: {e}")
        return {"status": "ok", "processed": results}


@app.route('/upload', methods=['POST', 'OPTIONS'])
def upload_and_process() -> Dict[str, Any]:
    """Legacy endpoint - now redirects to complete pipeline"""
    return upload_complete_pipeline()


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
        # Query params for performance controls
        page = max(1, int(request.args.get('page', '1') or '1'))
        page_size = max(1, min(200, int(request.args.get('page_size', '100') or '100')))
        light = request.args.get('light', '0') in ('1', 'true', 'True')
        refresh = request.args.get('refresh', '0') in ('1', 'true', 'True')

        cache_key = f"calls:page={page}:size={page_size}:light={int(light)}"
        if not refresh:
            # Check if blob storage has changed before using cache
            blob_changed = _check_blob_changes()
            if not blob_changed:
                # Use smart cache with long TTL (24 hours) - only invalidated on changes
                cached = _cache_get(cache_key, ttl_seconds=86400)  # 24 hours
                if cached is not None:
                    return cached

        # Ensure container exists
        azure_storage.ensure_container_exists(azure_storage.DEFAULT_CONTAINER)

        container = azure_storage.blob_service_client.get_container_client(azure_storage.DEFAULT_CONTAINER)

        # Gather audio blobs from both the configured folder and anywhere in the container
        audio_exts = (".mp3", ".wav", ".m4a", ".mp4")
        audio_blobs = list(container.list_blobs(name_starts_with=f"{azure_storage.AUDIO_FOLDER}/"))
        if not audio_blobs:
            # Fallback: scan entire container for audio extensions
            audio_blobs = [b for b in container.list_blobs() if any(b.name.lower().endswith(ext) for ext in audio_exts)]

        # Build a minimal index first: (call_id, audio_name, uploaded_at, blob)
        indexed: List[Dict[str, Any]] = []
        seen_call_ids: set[str] = set()
        for blob in audio_blobs:
            audio_path = blob.name
            audio_name = audio_path.split("/")[-1]
            call_id = audio_name.rsplit(".", 1)[0]
            if call_id in seen_call_ids:
                continue
            seen_call_ids.add(call_id)
            created = getattr(blob, "creation_time", None) or getattr(blob, "last_modified", None)
            indexed.append({
                "call_id": call_id,
                "audio_name": audio_name,
                "uploaded_at": created.isoformat() if isinstance(created, datetime) else None,
                "_blob": blob,
            })

        # newest first without loading analysis
        indexed.sort(key=lambda x: x.get("uploaded_at") or "", reverse=True)

        # Pagination window selection before any heavy I/O
        start = (page - 1) * page_size
        end = start + page_size
        window = indexed[start:end]

        entries: List[Dict[str, Any]] = []
        for item in window:
            call_id = item["call_id"]
            audio_name = item["audio_name"]
            uploaded_at = item["uploaded_at"]

            parsed = None
            first_analysis_path = None
            category = None
            attitude = None

            if not light:
                # Prefer persona analysis folder
                parsed, first_analysis_path = _persona_analysis_for_call(call_id)
                if not parsed:
                    # Fallback: any analysis folder
                    parsed, first_analysis_path = _first_analysis_for_call(call_id)
                category, attitude = _derive_category_and_attitude(parsed)
                if (category is None or attitude is None) and not parsed:
                    norm_id = call_id.replace(" ", "_")
                    if norm_id != call_id:
                        parsed, first_analysis_path = _persona_analysis_for_call(norm_id)
                        if not parsed:
                            parsed, first_analysis_path = _first_analysis_for_call(norm_id)
                        c2, a2 = _derive_category_and_attitude(parsed)
                        category = category or c2
                        attitude = attitude or a2

            entries.append({
                "audio_name": audio_name,
                "call_id": call_id,
                "uploaded_at": uploaded_at,
                "analysis": None if light else parsed,
                "call_category": category,
                "agent_attitude": attitude,
                "analysis_file": first_analysis_path,
            })

        _cache_set(cache_key, entries)
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


def _delete_call_assets(call_id: str) -> Dict[str, Any]:
    """Delete audio, transcription, and analysis JSONs for a given call id.
    Deletes from:
      - audios/<call_id>.(mp3|wav|m4a|mp4) (whichever exists)
      - transcriptions/<call_id>.txt
      - llmanalysis/default/<call_id>.json
      - llmanalysis/persona/<call_id>.json
      - Azure Search index document
    Returns a dict with details of what was deleted.
    """
    deleted: Dict[str, Any] = {
        "audio_path": None,
        "transcription": False,
        "analysis_default": False,
        "analysis_persona": False,
        "search_index": False,
    }

    # Delete audio by resolving full path then deleting by path
    try:
        audio_path = azure_storage.find_audio_blob_path_for_call_id(call_id)
        if audio_path:
            client = azure_storage.blob_service_client.get_blob_client(
                container=azure_storage.DEFAULT_CONTAINER, blob=audio_path
            )
            try:
                client.delete_blob()
                deleted["audio_path"] = audio_path
            except Exception:
                # best-effort
                pass
    except Exception:
        pass

    # Delete transcription
    try:
        azure_storage.delete_transcription(f"{call_id}.txt")
        deleted["transcription"] = True
    except Exception:
        pass

    # Delete analysis JSONs (default and persona)
    try:
        azure_storage.delete_blob(f"default/{call_id}.json", prefix=azure_storage.LLM_ANALYSIS_FOLDER)
        deleted["analysis_default"] = True
    except Exception:
        pass
    try:
        azure_storage.delete_blob(f"persona/{call_id}.json", prefix=azure_storage.LLM_ANALYSIS_FOLDER)
        deleted["analysis_persona"] = True
    except Exception:
        pass

    # Delete from Azure Search index
    try:
        success, message = azure_search.delete_document_from_index("marketing_sentiment_details", call_id)
        deleted["search_index"] = success
        if success:
            print(f"Deleted call '{call_id}' from search index")
        else:
            print(f"Failed to delete call '{call_id}' from search index: {message}")
    except Exception as e:
        print(f"Error deleting call '{call_id}' from search index: {e}")
        pass

    return deleted


@app.route('/calls/<call_id>', methods=['DELETE', 'OPTIONS'])
def delete_call(call_id: str) -> Dict[str, Any]:
    # Handle OPTIONS preflight request
    if request.method == 'OPTIONS':
        return {"status": "ok"}

    try:
        print(f"Deleting call '{call_id}' and all associated assets...")
        details = _delete_call_assets(call_id)

        # Force complete cache invalidation - clear all cached data
        print("Invalidating all caches after deletion...")
        _invalidate_cache()
        
        # Clear the blob signature cache to force blob change detection
        _CACHE.pop("_blob_signature", None)
        
        # Force recalculation of dashboard summary (no cache usage)
        print("Recalculating dashboard summary after deletion...")
        dashboard_data = calculate_dashboard_summary()
        
        # Ensure MongoDB is updated before considering delete complete
        mongo_success = False
        max_retries = 3
        for attempt in range(max_retries):
            try:
                mongo_success = mongo_upsert_dashboard_summary(dashboard_data)
                if mongo_success:
                    print(f"MongoDB dashboard update successful on attempt {attempt + 1}")
                    break
                else:
                    print(f"MongoDB dashboard update failed on attempt {attempt + 1}")
            except Exception as e:
                print(f"MongoDB update error on attempt {attempt + 1}: {e}")
            
            if attempt < max_retries - 1:
                import time
                time.sleep(0.5)  # Brief pause before retry
        
        if not mongo_success:
            print("WARNING: MongoDB dashboard update failed after all retries")
            # Still continue but log the issue
        
        # Update blob storage cache as backup
        try:
            save_dashboard_summary_to_blob(dashboard_data)
            print("Blob storage dashboard cache updated")
        except Exception as e:
            print(f"Blob storage update failed: {e}")

        # Verify MongoDB has the updated data
        try:
            verification = mongo_get_dashboard_summary()
            if verification:
                verified_count = verification.get('total_calls', -1)
                expected_count = dashboard_data.get('total_calls', -1)
                print(f"MongoDB verification: expected {expected_count} calls, found {verified_count} calls")
                if verified_count != expected_count:
                    print("WARNING: MongoDB verification failed - counts don't match")
            else:
                print("WARNING: Could not verify MongoDB data after update")
        except Exception as e:
            print(f"MongoDB verification error: {e}")

        print(f"Call '{call_id}' deletion completed. New totals: {dashboard_data.get('total_calls', 0)} calls, {dashboard_data.get('calls_with_analysis', 0)} with analysis")

        return {
            "status": "success",
            "message": f"Call '{call_id}' deleted successfully",
            "deleted": details,
            "dashboard": {
                "total_calls": dashboard_data.get("total_calls", 0),
                "calls_with_analysis": dashboard_data.get("calls_with_analysis", 0),
                "updated_at": dashboard_data.get("updated_at"),
            },
        }
    except Exception as e:
        print(f"Error deleting call '{call_id}': {e}")
        return {
            "status": "error",
            "message": str(e),
        }


@app.route('/dashboard/summary', methods=['GET'])
def dashboard_summary() -> Dict[str, Any]:
    """Return dashboard summary strictly from MongoDB.
    Always returns fresh data - no caching to ensure immediate updates after deletions.
    """
    try:
        print("Dashboard summary requested - checking MongoDB for latest data...")
        
        # Always get fresh data from MongoDB
        mongo_doc = mongo_get_dashboard_summary()
        
        if mongo_doc is not None:
            print(f"Found dashboard data in MongoDB: {mongo_doc.get('total_calls', 0)} calls, updated at {mongo_doc.get('updated_at', 'unknown')}")
            response = jsonify(mongo_doc)
            # Force no caching to ensure immediate updates after deletions
            response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
            response.headers['Pragma'] = 'no-cache'
            response.headers['Expires'] = '0'
            return response

        # Not found in Mongo: compute fresh data and persist
        print("No dashboard data in MongoDB - computing fresh summary...")
        result = calculate_dashboard_summary()
        
        # Ensure MongoDB is updated before returning
        try:
            mongo_success = mongo_upsert_dashboard_summary(result)
            if mongo_success:
                print(f"Successfully stored fresh dashboard data in MongoDB: {result.get('total_calls', 0)} calls")
            else:
                print("Failed to store dashboard data in MongoDB")
        except Exception as e:
            print(f"Error storing dashboard data in MongoDB: {e}")
        
        # Best-effort persist to blob as secondary store
        try:
            save_dashboard_summary_to_blob(result)
        except Exception as e:
            print(f"Error saving dashboard to blob: {e}")
        
        response = jsonify(result)
        # Force no caching
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
        return response
        
    except Exception as e:
        print(f"Error in dashboard_summary endpoint: {e}")
        return jsonify({
            "error": str(e),
            "total_calls": 0,
            "calls_with_analysis": 0
        }), 500

@app.route('/insights', methods=['GET'])
def get_insights() -> Dict[str, Any]:
    """Return precomputed overall insights directly from MongoDB without regeneration."""
    try:
        doc = mongo_get_dashboard_summary()
        if doc is None:
            # Bootstrap by computing once, persisting, then returning
            summary = calculate_dashboard_summary()
            try:
                mongo_upsert_dashboard_summary(summary)
            except Exception:
                pass
            doc = summary
        
        result = {
            "status": "ok",
            "comprehensive_insights": doc.get("overall_insights"),
            "total_calls": doc.get("total_calls", 0),
            "summaries_found": doc.get("calls_with_analysis", 0),
        }
        
        response = jsonify(result)
        # Add caching headers for better performance
        response.headers['Cache-Control'] = 'public, max-age=60'  # Cache for 1 minute
        response.headers['ETag'] = f'"{doc.get("updated_at", "unknown")}"'
        return response
        
    except Exception as e:
        result = {
            "status": "error",
            "message": str(e),
            "comprehensive_insights": None,
            "total_calls": 0,
            "summaries_found": 0,
        }
        response = jsonify(result)
        response.headers['Cache-Control'] = 'no-cache'
        return response

@app.route('/chat', methods=['POST', 'OPTIONS'])
def chat_with_data():
    """Chat with calls using Azure AI Search index 'marketing_sentiment_details' for retrieval, with long-chat handling.
    Body: { query: string, history?: [{role: 'user'|'ai', text: string}], top_k?: int }
    """
    # Handle OPTIONS preflight request
    if request.method == 'OPTIONS':
        return {"status": "ok"}
        
    payload: Dict[str, Any] = request.get_json(force=True) or {}
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


@app.route('/diagnostics/mongo', methods=['GET'])
def diagnose_mongo() -> Dict[str, Any]:
    """Check MongoDB connectivity and ensure dashboard collection/document exist.
    If the dashboard summary document does not exist, compute and upsert it.
    Returns connection info and a compact view of the stored summary.
    """
    try:
        uri = os.getenv("MONGO_URI") or "cosmos-default"
        db_name = os.getenv("MONGO_DB", "elaraby")
        coll_name = os.getenv("MONGO_DASHBOARD_COLLECTION", "dashboard_summaries")

        client = _get_mongo_client()
        if client is None:
            return {
                "status": "error",
                "message": "Unable to initialize Mongo client",
                "mongo_uri": uri,
                "db": db_name,
                "collection": coll_name,
            }

        coll = _get_dashboard_collection(client)
        if coll is None:
            return {
                "status": "error",
                "message": "Unable to access Mongo collection",
                "mongo_uri": uri,
                "db": db_name,
                "collection": coll_name,
            }

        # Try to fetch the latest summary
        doc = coll.find_one({"_id": "dashboard_summary_latest"})
        created = False
        if not doc:
            # Compute and upsert fresh summary
            fresh = calculate_dashboard_summary()
            mongo_upsert_dashboard_summary(fresh)
            doc = coll.find_one({"_id": "dashboard_summary_latest"})
            created = True

        if not doc:
            return {
                "status": "error",
                "message": "Dashboard summary document not found after upsert attempt",
                "mongo_uri": uri,
                "db": db_name,
                "collection": coll_name,
            }

        # Build compact response
        compact = {k: doc.get(k) for k in [
            "total_calls",
            "calls_with_analysis",
            "avg_sentiment",
            "resolved_rate",
            "avg_aht_seconds",
            "avg_talk_seconds",
            "avg_hold_seconds",
        ] if k in doc}

        # Include small samples of histogram keys for visibility
        def top_keys(d: Any, n: int = 5) -> Dict[str, Any]:
            if isinstance(d, dict):
                items = sorted(d.items(), key=lambda x: (-int(x[1]) if str(x[1]).isdigit() else 0, str(x[0])))
                return {k: v for k, v in items[:n]}
            return {}

        compact["sentiment_labels"] = top_keys(doc.get("sentiment_labels"), 5)
        compact["dispositions"] = top_keys(doc.get("dispositions"), 5)
        compact["categories"] = top_keys(doc.get("categories"), 5)
        compact["agent_professionalism"] = top_keys(doc.get("agent_professionalism"), 5)

        return {
            "status": "ok",
            "created_now": created,
            "mongo_uri": uri,
            "db": db_name,
            "collection": coll_name,
            "document_id": doc.get("_id", "dashboard_summary_latest"),
            "updated_at": str(doc.get("updated_at")),
            "summary_compact": compact,
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
        }

@app.route('/refresh-dashboard-cache', methods=['POST', 'OPTIONS'])
def refresh_dashboard_cache() -> Dict[str, Any]:
    """Manually refresh the dashboard summary cache in blob storage."""
    # Handle OPTIONS preflight request
    if request.method == 'OPTIONS':
        return {"status": "ok"}
        
    try:
        print("Manually refreshing dashboard summary cache...")
        
        # Calculate fresh dashboard summary
        dashboard_data = calculate_dashboard_summary()
        
        # Save to blob storage
        success = save_dashboard_summary_to_blob(dashboard_data)
        
        if success:
            return {
                "status": "success",
                "message": "Dashboard summary cache refreshed successfully",
                "total_calls": dashboard_data.get("total_calls", 0),
                "cached_at": datetime.utcnow().isoformat()
            }
        else:
            return {
                "status": "error",
                "message": "Failed to save dashboard summary to blob storage"
            }
            
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error refreshing dashboard cache: {str(e)}"
        }


@app.route('/refresh-calls-and-dashboard', methods=['POST', 'OPTIONS'])
def refresh_calls_and_dashboard_endpoint() -> Dict[str, Any]:
    """Manually refresh both calls list and dashboard summary after deletions."""
    # Handle OPTIONS preflight request
    if request.method == 'OPTIONS':
        return {"status": "ok"}
        
    return refresh_calls_and_dashboard()


@app.route('/invalidate-cache', methods=['POST', 'OPTIONS'])
def invalidate_cache_endpoint() -> Dict[str, Any]:
    """Manually invalidate all caches - use after deleting files from blob storage."""
    # Handle OPTIONS preflight request
    if request.method == 'OPTIONS':
        return {"status": "ok"}
        
    try:
        print("Manually invalidating all caches...")
        
        # Invalidate all caches using smart versioning
        _invalidate_cache()
        
        return {
            "status": "success",
            "message": "All caches invalidated successfully",
            "cache_version": _get_cache_version(),
            "invalidated_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error invalidating caches: {str(e)}"
        }


@app.route('/force-refresh-all', methods=['POST', 'OPTIONS'])
def force_refresh_all() -> Dict[str, Any]:
    """Force refresh all caches and data - use after uploads or deletions."""
    # Handle OPTIONS preflight request
    if request.method == 'OPTIONS':
        return {"status": "ok"}
        
    try:
        print("Force refreshing all caches and data...")
        
        # Invalidate all caches using smart versioning
        _invalidate_cache()
        
        # Calculate fresh dashboard summary
        dashboard_data = calculate_dashboard_summary()
        try:
            mongo_upsert_dashboard_summary(dashboard_data)
        except Exception:
            pass
        save_dashboard_summary_to_blob(dashboard_data)
        
        return {
            "status": "success",
            "message": "All caches invalidated and data refreshed successfully",
            "total_calls": dashboard_data.get("total_calls", 0),
            "calls_with_analysis": dashboard_data.get("calls_with_analysis", 0),
            "cache_version": _get_cache_version(),
            "refreshed_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error force refreshing all data: {str(e)}"
        }


@app.route('/reindex-all-calls', methods=['POST', 'OPTIONS'])
def reindex_all_calls() -> Dict[str, Any]:
    """Re-index all existing calls into the Azure Search index for chat functionality."""
    # Handle OPTIONS preflight request
    if request.method == 'OPTIONS':
        return {"status": "ok"}
        
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
                
                # Clear caches after successful reindexing
                try:
                    clear_calls_cache()
                    dashboard_data = calculate_dashboard_summary()
                    save_dashboard_summary_to_blob(dashboard_data)
                    print("Caches cleared after reindexing")
                except Exception as e:
                    print(f"Warning: Could not clear caches after reindexing: {e}")
                
                return {
                    "status": "success",
                    "message": f"Successfully re-indexed {indexed_count} documents",
                    "indexed_count": indexed_count,
                    "total_calls": len(calls),
                    "previous_count": current_count,
                    "new_count": new_count,
                    "caches_cleared": True
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


# ============================================================================
# Transcription Revision Endpoints
# ============================================================================

@app.route('/create-revised-arabic-transcription', methods=['POST'])
def create_revised_arabic_transcription():
    """
    Create enhanced, cleaned Arabic Egyptian conversation versions for specific call IDs.
    
    Expected JSON body:
    {
        "call_ids": ["call_001", "call_002"],  // Optional, if not provided processes all
        "force_regenerate": false  // Optional, default false
    }
    """
    try:
        data = request.get_json() if request.is_json else {}
        call_ids = data.get('call_ids', [])
        force_regenerate = data.get('force_regenerate', False)
        
        if call_ids:
            # Process specific call IDs
            results = []
            for call_id in call_ids:
                print(f"Processing Arabic revision for call: {call_id}")
                
                # Read original transcription
                original_transcription = azure_storage.read_transcription(f"{call_id}.txt")
                if not original_transcription:
                    results.append({
                        "call_id": call_id,
                        "status": "error",
                        "message": f"Original transcription not found for {call_id}"
                    })
                    continue
                
                # Check if already exists
                if not force_regenerate and azure_storage.revised_arabic_transcription_already_exists(call_id):
                    results.append({
                        "call_id": call_id,
                        "status": "skipped",
                        "message": f"Revised Arabic transcription already exists for {call_id}"
                    })
                    continue
                
                # Get call analysis for context
                call_analysis = None
                try:
                    call_analysis = azure_storage.read_llm_analysis("persona", f"{call_id}.json")
                except:
                    pass
                
                # Create revised transcription
                try:
                    revised_arabic = revision_service.create_revised_arabic_transcription(
                        original_transcription, call_analysis
                    )
                    
                    # Save to blob storage
                    azure_storage.upload_revised_arabic_transcription_to_blob(call_id, revised_arabic)
                    
                    results.append({
                        "call_id": call_id,
                        "status": "success",
                        "message": f"Revised Arabic transcription created for {call_id}",
                        "blob_path": f"{azure_storage.REVISED_ARABIC_FOLDER}/{call_id}.txt"
                    })
                    
                except Exception as e:
                    results.append({
                        "call_id": call_id,
                        "status": "error",
                        "message": f"Error creating Arabic revision: {str(e)}"
                    })
            
            return {
                "status": "completed",
                "processed": len(call_ids),
                "results": results
            }
        
        else:
            # Process all transcriptions
            print("Processing Arabic revisions for all transcriptions...")
            batch_results = revision_service.process_all_transcriptions(force_regenerate)
            
            return {
                "status": "completed",
                "message": "Batch processing completed for Arabic revisions",
                "total_processed": batch_results["processed"],
                "arabic_successes": batch_results["arabic_success"],
                "errors": len(batch_results["errors"]),
                "error_details": batch_results["errors"][:10]  # Limit error details
            }
            
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error processing Arabic revision request: {str(e)}"
        }, 500


@app.route('/create-revised-english-transcription', methods=['POST'])
def create_revised_english_transcription():
    """
    Create cleaned English conversation versions for specific call IDs.
    
    Expected JSON body:
    {
        "call_ids": ["call_001", "call_002"],  // Optional, if not provided processes all
        "force_regenerate": false  // Optional, default false
    }
    """
    try:
        data = request.get_json() if request.is_json else {}
        call_ids = data.get('call_ids', [])
        force_regenerate = data.get('force_regenerate', False)
        
        if call_ids:
            # Process specific call IDs
            results = []
            for call_id in call_ids:
                print(f"Processing English revision for call: {call_id}")
                
                # Read original transcription
                original_transcription = azure_storage.read_transcription(f"{call_id}.txt")
                if not original_transcription:
                    results.append({
                        "call_id": call_id,
                        "status": "error",
                        "message": f"Original transcription not found for {call_id}"
                    })
                    continue
                
                # Check if already exists
                if not force_regenerate and azure_storage.revised_english_transcription_already_exists(call_id):
                    results.append({
                        "call_id": call_id,
                        "status": "skipped",
                        "message": f"Revised English transcription already exists for {call_id}"
                    })
                    continue
                
                # Get call analysis for context
                call_analysis = None
                try:
                    call_analysis = azure_storage.read_llm_analysis("persona", f"{call_id}.json")
                except:
                    pass
                
                # Create revised transcription
                try:
                    revised_english = revision_service.create_revised_english_transcription(
                        original_transcription, call_analysis
                    )
                    
                    # Save to blob storage
                    azure_storage.upload_revised_english_transcription_to_blob(call_id, revised_english)
                    
                    results.append({
                        "call_id": call_id,
                        "status": "success",
                        "message": f"Revised English transcription created for {call_id}",
                        "blob_path": f"{azure_storage.REVISED_ENGLISH_FOLDER}/{call_id}.txt"
                    })
                    
                except Exception as e:
                    results.append({
                        "call_id": call_id,
                        "status": "error",
                        "message": f"Error creating English revision: {str(e)}"
                    })
            
            return {
                "status": "completed",
                "processed": len(call_ids),
                "results": results
            }
        
        else:
            # Process all transcriptions
            print("Processing English revisions for all transcriptions...")
            batch_results = revision_service.process_all_transcriptions(force_regenerate)
            
            return {
                "status": "completed",
                "message": "Batch processing completed for English revisions",
                "total_processed": batch_results["processed"],
                "english_successes": batch_results["english_success"],
                "errors": len(batch_results["errors"]),
                "error_details": batch_results["errors"][:10]  # Limit error details
            }
            
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error processing English revision request: {str(e)}"
        }, 500


@app.route('/create-both-revised-transcriptions', methods=['POST'])
def create_both_revised_transcriptions():
    """
    Create both Arabic and English revised transcriptions for specific call IDs or all calls.
    
    Expected JSON body:
    {
        "call_ids": ["call_001", "call_002"],  // Optional, if not provided processes all
        "force_regenerate": false  // Optional, default false
    }
    """
    try:
        data = request.get_json() if request.is_json else {}
        call_ids = data.get('call_ids', [])
        force_regenerate = data.get('force_regenerate', False)
        
        if call_ids:
            # Process specific call IDs
            results = []
            for call_id in call_ids:
                print(f"Processing both revisions for call: {call_id}")
                
                arabic_success, english_success, message = revision_service.process_single_transcription(
                    call_id, force_regenerate
                )
                
                results.append({
                    "call_id": call_id,
                    "arabic_success": arabic_success,
                    "english_success": english_success,
                    "message": message,
                    "status": "success" if arabic_success and english_success else "partial" if arabic_success or english_success else "error"
                })
            
            return {
                "status": "completed",
                "processed": len(call_ids),
                "results": results
            }
        
        else:
            # Process all transcriptions
            print("Processing both Arabic and English revisions for all transcriptions...")
            batch_results = revision_service.process_all_transcriptions(force_regenerate)
            
            return {
                "status": batch_results["status"],
                "message": batch_results["message"],
                "total_processed": batch_results["processed"],
                "arabic_successes": batch_results["arabic_success"],
                "english_successes": batch_results["english_success"],
                "errors": len(batch_results["errors"]),
                "error_details": batch_results["errors"][:10]  # Limit error details
            }
            
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error processing revision request: {str(e)}"
        }, 500


@app.route('/get-revised-transcriptions/<call_id>', methods=['GET'])
def get_revised_transcriptions(call_id: str):
    """
    Get all transcription versions (original, Arabic revised, English revised) for a specific call.
    This endpoint is designed for the CallDetails page with three tabs.
    """
    try:
        result = {
            "call_id": call_id,
            "transcriptions": {},
            "available_tabs": []
        }
        
        # Get original transcription
        try:
            original = azure_storage.read_transcription(f"{call_id}.txt")
            if original:
                result["transcriptions"]["original"] = original
                result["available_tabs"].append("original")
        except Exception as e:
            print(f"Error reading original transcription: {e}")
        
        # Get revised Arabic transcription
        try:
            revised_arabic = azure_storage.read_revised_arabic_transcription(f"{call_id}.txt")
            if revised_arabic:
                result["transcriptions"]["revised_arabic"] = revised_arabic
                result["available_tabs"].append("revised_arabic")
        except Exception as e:
            print(f"Error reading Arabic revision: {e}")
        
        # Get revised English transcription
        try:
            revised_english = azure_storage.read_revised_english_transcription(f"{call_id}.txt")
            if revised_english:
                result["transcriptions"]["revised_english"] = revised_english
                result["available_tabs"].append("revised_english")
        except Exception as e:
            print(f"Error reading English revision: {e}")
        
        if not result["available_tabs"]:
            return {
                "status": "error",
                "message": f"No transcriptions found for call {call_id}"
            }, 404
        
        result["status"] = "success"
        return result
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error retrieving transcriptions: {str(e)}"
        }, 500


if __name__ == "__main__":
    app.run(debug=True)
