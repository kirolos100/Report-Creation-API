import os
import time
from datetime import datetime
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

SPEECH_ENDPOINT_DEFAULT = "https://uaenorth.api.cognitive.microsoft.com/"
SPEECH_REGION_DEFAULT = "uaenorth"
SPEECH_KEY_DEFAULT = "5bVGgxC4rjSjBhKgngZDLdSm5cLiNida4vXJ8vEIWQi608yOQj1GJQQJ99BGACF24PCXJ3w3AAAYACOGnyyd"


def _get_speech_base_url() -> str:
    endpoint = (os.getenv("AZURE_SPEECH_ENDPOINT", "") or SPEECH_ENDPOINT_DEFAULT).strip().rstrip("/")
    region = (os.getenv("AZURE_SPEECH_REGION", "") or SPEECH_REGION_DEFAULT).strip()
    if endpoint:
        return endpoint
    if not region:
        raise ValueError("AZURE_SPEECH_REGION or AZURE_SPEECH_ENDPOINT must be set.")
    return f"https://{region}.api.cognitive.microsoft.com"


def _ticks_to_timestamp(ticks: float) -> str:
    # Speech service uses 100-nanosecond ticks
    total_ms = int(round(float(ticks) / 10000.0))
    hours = total_ms // 3600000
    minutes = (total_ms % 3600000) // 60000
    seconds = (total_ms % 60000) // 1000
    millis = total_ms % 1000
    return f"[{hours:02d}:{minutes:02d}:{seconds:02d}.{millis:03d}]"


def transcribe_with_speech_batch(
    audio_sas_url: str,
    locale: str = "ar-EG",
    use_stereo_audio: bool = False,
    poll_seconds: int = 10,
) -> str:
    """
    Submit a batch transcription job and return a timestamped plain-text transcript.
    """
    key = os.getenv("AZURE_SPEECH_KEY") or SPEECH_KEY_DEFAULT
    if not key:
        raise ValueError("AZURE_SPEECH_KEY must be set.")

    base = _get_speech_base_url()
    path = "/speechtotext/v3.2/transcriptions"
    url = f"{base}{path}"

    headers = {
        "Ocp-Apim-Subscription-Key": key,
        "Content-Type": "application/json",
    }

    # Enhanced audio format detection and validation
    audio_ext = audio_sas_url.lower().split('.')[-1].split('?')[0]  # Remove query params
    supported_formats = ['mp3', 'wav', 'm4a', 'mp4', 'aac', 'ogg']
    
    if audio_ext not in supported_formats:
        raise ValueError(f"Unsupported audio format: {audio_ext}. Supported formats: {', '.join(supported_formats)}")

    body = {
        "contentUrls": [audio_sas_url],
        "properties": {
            "diarizationEnabled": (not use_stereo_audio),
            "timeToLive": "PT30M",
            "wordLevelTimestampsEnabled": False,
            "displayFormWordLevelTimestampsEnabled": False,
            "channels": [0, 1] if use_stereo_audio else [0],
            "punctuationMode": "DictatedAndAutomatic",
            "profanityFilterMode": "Masked",
        },
        "locale": locale,
        "displayName": f"call_center_{datetime.utcnow().isoformat()}",
    }

    create = requests.post(url, headers=headers, json=body)
    if create.status_code not in (201, 202):
        error_msg = f"Create transcription failed: {create.status_code}"
        try:
            error_detail = create.json()
            if "error" in error_detail:
                error_msg += f" - {error_detail['error'].get('message', 'Unknown error')}"
        except:
            error_msg += f" - {create.text}"
        raise RuntimeError(error_msg)

    transcription_url = create.json().get("self")
    if not transcription_url:
        raise RuntimeError(f"Unexpected create response: {create.text}")

    # Poll with better error handling
    max_poll_attempts = 60  # 10 minutes max wait time
    poll_count = 0
    
    while poll_count < max_poll_attempts:
        time.sleep(poll_seconds)
        poll_count += 1
        
        try:
            status_resp = requests.get(transcription_url, headers=headers)
            if status_resp.status_code != 200:
                raise RuntimeError(f"Status check failed: {status_resp.status_code} {status_resp.text}")
            
            status_data = status_resp.json()
            status = (status_data.get("status") or "").lower()
            
            if status == "failed":
                # Extract detailed error information
                error_info = status_data.get("properties", {}).get("error", {})
                error_code = error_info.get("code", "Unknown")
                error_message = error_info.get("message", "No error details available")
                raise RuntimeError(f"Transcription failed: {error_code} - {error_message}")
            
            if status == "succeeded":
                break
                
            if poll_count >= max_poll_attempts:
                raise RuntimeError(f"Transcription timed out after {max_poll_attempts * poll_seconds} seconds")
                
        except Exception as e:
            if "Transcription failed" in str(e):
                raise e
            if poll_count >= max_poll_attempts:
                raise RuntimeError(f"Transcription polling failed after {max_poll_attempts} attempts: {str(e)}")
            continue

    # Get files
    files_url = f"{transcription_url}/files"
    files_resp = requests.get(files_url, headers=headers)
    if files_resp.status_code != 200:
        raise RuntimeError(f"List files failed: {files_resp.status_code} {files_resp.text}")

    values = files_resp.json().get("values", [])
    content_url: Optional[str] = None
    for v in values:
        kind = (v.get("kind") or "").lower()
        if kind == "transcription":
            links = v.get("links") or {}
            content_url = links.get("contentUrl")
            break
    if not content_url:
        raise RuntimeError(f"Could not find transcription contentUrl in {files_resp.text}")

    content_resp = requests.get(content_url)
    if content_resp.status_code != 200:
        raise RuntimeError(f"Fetch content failed: {content_resp.status_code} {content_resp.text}")

    data = content_resp.json()
    phrases = data.get("recognizedPhrases", [])
    
    if not phrases:
        raise RuntimeError("No transcription phrases found in response")
    
    # Sort by offsetInTicks
    phrases = sorted(phrases, key=lambda p: p.get("offsetInTicks", 0))

    lines = []
    for p in phrases:
        nbest = (p.get("nBest") or [])
        text = (nbest[0].get("display") if nbest else "").strip()
        ts = _ticks_to_timestamp(p.get("offsetInTicks", 0))
        if text:
            lines.append(f"{ts} {text}")

    if not lines:
        raise RuntimeError("No valid transcription lines generated")

    return "\n".join(lines)


