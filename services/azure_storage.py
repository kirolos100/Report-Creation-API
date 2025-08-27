import os
import json
import mimetypes
from datetime import datetime, timedelta
from typing import Dict, Any

from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContentSettings, BlobBlock, generate_blob_sas, BlobSasPermissions
from azure.storage.queue import QueueClient

load_dotenv()

# Environment / configuration
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")

DEFAULT_CONTAINER = os.getenv("DEFAULT_CONTAINER", "mainproject")
AUDIO_FOLDER = os.getenv("AUDIO_FOLDER", "audios")
TRANSCRIPTION_FOLDER = os.getenv("TRANSCRIPTION_FOLDER", "transcriptions")
EVAL_FOLDER = os.getenv("EVAL_FOLDER", "evals")
PROMPT_FOLDER = os.getenv("PROMPT_FOLDER", "prompts")
LLM_ANALYSIS_FOLDER = os.getenv("LLM_ANALYSIS_FOLDER", "llmanalysis")
STORAGE_QUEUE_NAME = os.getenv("STORAGE_QUEUE_NAME", "integration-queue")

# Flexible auth: FORCE using your provided connection string
AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=nidallhxy634olzi4;AccountKey=LfQXoW+2L4UPzv+W+Pu+VSffj1u1FjlKGkxa8Nf8OlqADHDRMz11pegIC+OCDmSfILrBTL01i0hH+AStUN5cVg==;EndpointSuffix=core.windows.net"

# Always use connection string based clients
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
credential = None
blob_account_url = None
queue_account_url = None
try:
    print("azure_storage: using connection string auth (forced)")
except Exception:
    pass


def ensure_container_exists(container_name: str = DEFAULT_CONTAINER):
    """
    Ensure the specified container exists; if not, create it.
    """
    try:
        container_client = blob_service_client.get_container_client(container_name)
        container_client.get_container_properties()
    except Exception as e:
        if "ContainerNotFound" in str(e):
            blob_service_client.create_container(container_name)
        else:
            raise e


def get_blob_client(blob_name: str, prefix: str = "", container_name: str = DEFAULT_CONTAINER):
    """
    Return the BlobClient for a given blob name and prefix within a container.
    """
    path = f"{prefix}/{blob_name}" if prefix else blob_name
    return blob_service_client.get_blob_client(container=container_name, blob=path)


def list_blobs(prefix: str = "", container_name: str = DEFAULT_CONTAINER):
    """
    List blobs within a container, optionally filtered by a prefix.
    Returns only the final part of the blob name (file name).
    """
    ensure_container_exists(container_name)
    container_client = blob_service_client.get_container_client(container_name)
    blob_list = container_client.list_blobs(name_starts_with=prefix)
    return [blob.name.split("/")[-1] for blob in blob_list]


def upload_blob(data, blob_name: str, prefix: str = "", container_name: str = DEFAULT_CONTAINER):
    """
    Upload the given data (file-like or bytes/string) to a blob name within a container/prefix.
    Overwrites if it exists.
    """
    if data is None:
        return "No data to upload."
    ensure_container_exists(container_name)
    client = get_blob_client(blob_name, prefix, container_name)
    # Prepare data and content type
    content_type, _ = mimetypes.guess_type(blob_name)
    content_settings = ContentSettings(content_type=content_type or "application/octet-stream")
    # Prefer streaming upload for large files to avoid timeouts
    payload = data
    if hasattr(data, "read"):
        try:
            data.seek(0)
        except Exception:
            pass
        # Pass the file-like object directly to enable SDK chunked upload
        payload = data
    try:
        client.upload_blob(
            payload,
            overwrite=True,
            content_settings=content_settings,
            timeout=900,
        )
    except Exception as e:
        # Fallback: manual block upload in 1MB chunks
        try:
            if hasattr(payload, "read"):
                try:
                    payload.seek(0)
                except Exception:
                    pass
                stream = payload
            else:
                import io
                stream = io.BytesIO(payload if isinstance(payload, (bytes, bytearray)) else bytes(payload))

            block_ids = []
            chunk_size = 1024 * 1024  # 1 MB
            index = 0
            while True:
                chunk = stream.read(chunk_size)
                if not chunk:
                    break
                block_id = (f"block-{index:07d}").encode("utf-8")
                client.stage_block(block_id=block_id, data=chunk, timeout=900)
                block_ids.append(BlobBlock(block_id=block_id))
                index += 1

            # commit the list of blocks
            client.commit_block_list(block_ids, content_settings=content_settings, timeout=900)
        except Exception as e2:
            path = f"{prefix}/{blob_name}" if prefix else blob_name
            print(f"Error uploading blob: container='{container_name}', path='{path}': {e}; fallback failed: {e2}")
            raise
    return f"Uploaded file to: {prefix}/{blob_name}" if prefix else f"Uploaded file to: {blob_name}"


def download_blob_to_local_file(blob_name: str, prefix: str = "", local_path: str = None, overwrite: bool = False):
    """
    Download a blob to a local file path. If local_path is not provided,
    it defaults to using the same file name as the blob_name in the current directory.
    """
    if not local_path:
        local_path = blob_name  # Use blob_name as the default local file name

    # Ensure we have a valid path
    if not local_path or local_path.strip() == "":
        local_path = f"tmp_{blob_name}"  # Fallback name if empty

    directory = os.path.dirname(local_path)

    # Create the directory if it doesn't exist and it's not empty
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
    
    if not overwrite and os.path.exists(local_path):
        return local_path

    client = get_blob_client(blob_name, prefix)
    # combine local working dir with local_path
    local_path = os.path.join(os.getcwd(), local_path)
    
    # Ensure the directory exists for the final path
    final_directory = os.path.dirname(local_path)
    if final_directory and not os.path.exists(final_directory):
        os.makedirs(final_directory)
    
    with open(local_path, "wb") as file_obj:
        download_stream = client.download_blob()
        file_obj.write(download_stream.readall())

    return local_path


def read_blob(blob_name: str, prefix: str = ""):
    """
    Read blob content as text (UTF-8).
    """
    try:
        client = get_blob_client(blob_name, prefix)
        download_stream = client.download_blob()
        return download_stream.readall().decode("utf-8")
    except Exception as e:
        try:
            path = f"{prefix}/{blob_name}" if prefix else blob_name
            print(f"Error reading blob: container='{DEFAULT_CONTAINER}', path='{path}': {e}")
        except Exception:
            print(f"Error reading blob: {e}")
        return None


def delete_blob(blob_name: str, prefix: str = ""):
    """
    Delete a blob from the container/prefix.
    """
    client = get_blob_client(blob_name, prefix)
    client.delete_blob()
    return f"Deleted blob: {prefix}/{blob_name}" if prefix else f"Deleted blob: {blob_name}"


def update_blob(blob_name: str, updated_content, prefix: str = ""):
    """
    Overwrite a blob with new text/binary content.
    """
    return upload_blob(updated_content, blob_name, prefix)


# ----------------------------------------------------------------------------
# Convenience functions for specific folders
# ----------------------------------------------------------------------------

def list_audios():
    return list_blobs(AUDIO_FOLDER)


def list_evals(prompt_name):
    """
    List all JSON evals under /EVAL_FOLDER/<prompt_no_ext>/
    """
    prompt_no_ext = prompt_name.split('.')[0]
    prefix = f"{EVAL_FOLDER}/{prompt_no_ext}"
    return list_blobs(prefix)

def list_transcriptions():
    return list_blobs(TRANSCRIPTION_FOLDER)

def list_prompts():
    all_prompts = list_blobs(PROMPT_FOLDER)
    # Filter out config files
    return [p for p in all_prompts if "__config" not in p]


def upload_audio_to_blob(file):
    # file should be an open file-like object or an UploadFile (FastAPI, etc.)
    name_no_spaces = file.name.replace(" ", "_")
    return upload_blob(file, name_no_spaces, AUDIO_FOLDER)

def upload_prompt_to_blob(file):
    return upload_blob(file, file.name, PROMPT_FOLDER)
    
def download_audio_to_local_file(blob_name):
    return download_blob_to_local_file(blob_name, AUDIO_FOLDER, "./tmp/" + blob_name)

def delete_audio(blob_name):
    return delete_blob(blob_name, AUDIO_FOLDER)

def read_transcription(blob_name):
    return read_blob(blob_name, TRANSCRIPTION_FOLDER)

def delete_transcription(blob_name):
    return delete_blob(blob_name, TRANSCRIPTION_FOLDER)

def read_prompt(blob_name):
    return read_blob(blob_name, PROMPT_FOLDER)

def update_prompt(blob_name, updated_content):
    return update_blob(blob_name, updated_content, PROMPT_FOLDER)

def _parse_account_from_conn_str(conn_str: str):
    try:
        parts = conn_str.split(";")
        kv = {}
        for p in parts:
            if not p:
                continue
            if "=" in p:
                k, v = p.split("=", 1)
                kv[k.strip()] = v.strip()
        return kv.get("AccountName"), kv.get("AccountKey")
    except Exception:
        return None, None

def get_audio_blob_sas_url(blob_name: str, expiry_minutes: int = 60) -> str:
    """
    Generate a read-only SAS URL for an audio blob for use by external services (e.g., Speech Batch API).
    """
    ensure_container_exists(DEFAULT_CONTAINER)
    # Normalize name like when uploading
    name_no_spaces = blob_name.split('/')[-1].replace(" ", "_")
    blob_path = f"{AUDIO_FOLDER}/{name_no_spaces}"

    account_name_env = STORAGE_ACCOUNT_NAME
    account_name_cs, account_key_cs = _parse_account_from_conn_str(AZURE_STORAGE_CONNECTION_STRING)
    account_name = account_name_env or account_name_cs
    account_key = account_key_cs

    if not (account_name and account_key):
        raise ValueError("Cannot create SAS URL: storage account name/key not available.")

    sas = generate_blob_sas(
        account_name=account_name,
        container_name=DEFAULT_CONTAINER,
        blob_name=blob_path,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() - timedelta(seconds=1) + timedelta(minutes=expiry_minutes),
    )

    client = blob_service_client.get_blob_client(container=DEFAULT_CONTAINER, blob=blob_path)
    return f"{client.url}?{sas}"


def get_blob_sas_url_for_path(blob_path: str, expiry_minutes: int = 60) -> str:
    """
    Generate a read-only SAS URL for an arbitrary blob path inside DEFAULT_CONTAINER.
    The caller must pass the full path within the container, e.g. "audios/file.mp3" or "some/other/folder/file.wav".
    """
    ensure_container_exists(DEFAULT_CONTAINER)

    account_name_env = STORAGE_ACCOUNT_NAME
    account_name_cs, account_key_cs = _parse_account_from_conn_str(AZURE_STORAGE_CONNECTION_STRING)
    account_name = account_name_env or account_name_cs
    account_key = account_key_cs

    if not (account_name and account_key):
        raise ValueError("Cannot create SAS URL: storage account name/key not available.")

    sas = generate_blob_sas(
        account_name=account_name,
        container_name=DEFAULT_CONTAINER,
        blob_name=blob_path,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() - timedelta(seconds=1) + timedelta(minutes=expiry_minutes),
    )

    client = blob_service_client.get_blob_client(container=DEFAULT_CONTAINER, blob=blob_path)
    return f"{client.url}?{sas}"


def list_audio_blobs_anywhere() -> list[str]:
    """
    Scan the entire container for blobs with common audio extensions, regardless of folder.
    Returns the full blob paths.
    """
    ensure_container_exists(DEFAULT_CONTAINER)
    container_client = blob_service_client.get_container_client(DEFAULT_CONTAINER)
    audio_exts = (".mp3", ".wav", ".m4a", ".mp4")
    paths = []
    for blob in container_client.list_blobs():
        name_lower = blob.name.lower()
        if any(name_lower.endswith(ext) for ext in audio_exts):
            paths.append(blob.name)
    return paths


def find_audio_blob_path_for_call_id(call_id: str) -> str | None:
    """
    Best-effort to find the audio blob path for a given call_id (file name without extension).
    Checks the configured AUDIO_FOLDER first, then scans the whole container.
    Returns the full blob path (e.g., "audios/call.mp3") or None.
    """
    audio_exts = (".mp3", ".wav", ".m4a", ".mp4")
    # Try configured folder first
    for ext in audio_exts:
        candidate = f"{AUDIO_FOLDER}/{call_id}{ext}"
        client = blob_service_client.get_blob_client(container=DEFAULT_CONTAINER, blob=candidate)
        try:
            client.get_blob_properties()
            return candidate
        except Exception:
            pass
    # Fallback: scan entire container
    for path in list_audio_blobs_anywhere():
        filename = path.split("/")[-1]
        if filename.rsplit(".", 1)[0] == call_id:
            return path
    return None

def upload_transcription_to_blob(blob_name, transcribed_text):
    """
    Upload a transcription as a .txt file in the TRANSCRIPTION_FOLDER.
    """
    # Clean up any spaces, etc.
    transcription_file_name = blob_name.split('/')[-1].replace(" ", "_") + ".txt"
    return upload_blob(transcribed_text, transcription_file_name, TRANSCRIPTION_FOLDER)


def transcription_already_exists(blob_name: str):
    """
    Check if a transcription for `blob_name` (as .txt) already exists.
    """
    transcription_file_name = blob_name + ".txt"
    return transcription_file_name in list_blobs(TRANSCRIPTION_FOLDER)


def get_calls_to_transcribe():
    calls = list_audios()
    total_calls = len(calls)
    total_transcribed = 0
    call_to_be_transcribed = []
    for call in calls:
        call_id = call.split('.')[0]
        if transcription_already_exists(call_id):
            total_transcribed += 1
        else:
            call_to_be_transcribed.append(call_id)

    return call_to_be_transcribed, total_transcribed, total_calls
# ----------------------------------------------------------------------------
# Prompt config helpers
# ----------------------------------------------------------------------------

def upload_prompt_config(prompt_name, config):
    """
    Upload a config for a given prompt as a normal string.
    The config blob name is <prompt_no_ext>__config.txt.
    """
    config_blob_name = prompt_name.split('.')[0] + "__config.txt"
    save = ",".join(config)
    return upload_blob(save, config_blob_name, PROMPT_FOLDER)


def read_prompt_config(blob_name):
    """
    Read the JSON config for a given prompt (if it exists).
    """
    config_blob_name = blob_name.split('.')[0] + "__config.txt"
    try:
        content = read_blob(config_blob_name, PROMPT_FOLDER)
        return content.split(",")
    except Exception:
        return None

def read_config():
    """
    Read the JSON config for transcription models or LLMs ect..
    """
    config_blob_name = "app_config.json"
    try:
        content = read_blob(config_blob_name, None)
        return json.loads(content)
    except Exception:
        return None

def save_config(config):
    """
    Save the JSON config for transcription models or LLMs ect..
    """
    config_blob_name = "app_config.json"
    data_to_upload = json.dumps(config)
    return upload_blob(data_to_upload, config_blob_name, "")
# ----------------------------------------------------------------------------
# LLM Analysis Listing/Reading
# ----------------------------------------------------------------------------

def list_llmanalysis(prompt_name):
    """
    List all JSON analyses under /LLM_ANALYSIS_FOLDER/<prompt_no_ext>/
    """
    prompt_no_ext = prompt_name.split('.')[0]
    prefix = f"{LLM_ANALYSIS_FOLDER}/{prompt_no_ext}"
    return list_blobs(prefix)


def read_llm_analysis(prompt_name: str, file_name: str) -> dict:
    """
    Load an LLM analysis file (JSON) from the container.
    """
    prompt_no_ext = prompt_name.split('.')[0]
    prefix = f"{LLM_ANALYSIS_FOLDER}/{prompt_no_ext}"
    try:
        content = read_blob(file_name, prefix)
        return json.loads(content)
    except:
        return {}

def read_eval(prompt_name: str, file_name: str) -> dict:
    """
    Load an LLM analysis file (JSON) from the container.
    """
    prompt_no_ext = prompt_name.split('.')[0]
    prefix = f"{EVAL_FOLDER}/{prompt_no_ext}"
    try:
        content = read_blob(file_name, prefix)
        return json.loads(content)
    except:
        return {}

def upload_llm_analysis_to_blob(name, prompt, analysis):
    """
    For storing analysis in JSON under /LLM_ANALYSIS_FOLDER/<prompt_name>/<name_no_ext>.json
    """
    prompt_name_no_ext = prompt.split('.')[0]
    call_id = name.split('.')[0]
    analysis_path = f"{prompt_name_no_ext}/{call_id}.json"
    full_prefix = LLM_ANALYSIS_FOLDER

    try:
        # Convert `analysis` to JSON if it's a Python dict
        data_to_upload = analysis if isinstance(analysis, str) else json.dumps(analysis)
        return upload_blob(data_to_upload, analysis_path, full_prefix)
    except Exception as e:
        return f"An error occurred while uploading LLM analysis: {e}"

def upload_eval_to_blob(name, prompt, evaluation):
    """
    For storing evals in JSON under /EVAL_FOLDER/<prompt_name>/<name_no_ext>.json
    """
    prompt_name_no_ext = prompt.split('.')[0]
    call_id = name.split('.')[0]
    eval_path = f"{prompt_name_no_ext}/{call_id}.json"
    full_prefix = EVAL_FOLDER

    try:
        # Convert `analysis` to JSON if it's a Python dict
        data_to_upload = evaluation if isinstance(evaluation, str) else json.dumps(evaluation)
        return upload_blob(data_to_upload, eval_path, full_prefix)
    except Exception as e:
        return f"An error occurred while uploading eval: {e}"
    

def get_uri(blob_name: str, prefix: str = "", container_name: str = DEFAULT_CONTAINER):
    """
    Get the URI for a blob in a container.
    """
    client = get_blob_client(blob_name, prefix, container_name)
    return client.url


def _create_queue_client(queue_name: str = STORAGE_QUEUE_NAME):
    """
    Create a QueueClient using the same auth strategy as blobs.
    """
    return QueueClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING, queue_name)

def ensure_queue_exists(queue_name: str = STORAGE_QUEUE_NAME):
    """
    Ensure the specified queue exists. Creates it if it does not.
    """
    queue_client = _create_queue_client(queue_name)
    try:
        queue_client.create_queue()
    except Exception as e:
        if 'QUEUE_ALREADY_EXISTS' in str(e) or 'QueueAlreadyExists' in str(e):
            pass
        else:
            raise e

def get_queue_client(queue_name: str = STORAGE_QUEUE_NAME):
    """
    Return the QueueClient for the given queue name, ensuring it exists.
    """
    ensure_queue_exists(queue_name)
    return _create_queue_client(queue_name)

def send_message_to_queue(message: str, queue_name: str = STORAGE_QUEUE_NAME):
    """
    Send a message to the specified queue.
    """
    queue_client = get_queue_client(queue_name)
    response = queue_client.send_message(message)
    return f"Sent message to queue '{queue_name}' with message id: {response.id}"

def validate_audio_file_format(blob_name: str, prefix: str = "") -> tuple[bool, str]:
    """
    Validate audio file format and provide recommendations for Azure Speech services.
    Returns (is_valid, message).
    """
    try:
        # Check file extension
        audio_exts = ('.mp3', '.wav', '.m4a', '.mp4', '.aac', '.ogg')
        file_ext = os.path.splitext(blob_name.lower())[1]
        
        if file_ext not in audio_exts:
            return False, f"Unsupported audio format: {file_ext}. Supported formats: {', '.join(audio_exts)}"
        
        # Download file to check content
        try:
            local_file = download_blob_to_local_file(blob_name, prefix)
        except Exception as e:
            return False, f"Failed to download audio file: {str(e)}"
        
        if not local_file or not os.path.exists(local_file):
            return False, f"Audio file not found locally after download: {blob_name}"
        
        # Check file size
        file_size = os.path.getsize(local_file)
        if file_size < 1024:  # 1KB minimum
            return False, f"Audio file too small: {file_size} bytes (minimum 1KB required)"
        if file_size > 100 * 1024 * 1024:  # 100MB maximum
            return False, f"Audio file too large: {file_size} bytes (maximum 100MB allowed)"
        
        # Check if file is readable
        try:
            with open(local_file, 'rb') as f:
                # Read first few bytes to check header
                header = f.read(16)
                if len(header) < 4:
                    return False, f"Audio file appears to be empty or corrupted: {blob_name}"
                
                # Basic format detection (simplified)
                if file_ext == '.mp3' and not header.startswith(b'\xff\xfb') and not header.startswith(b'ID3'):
                    return False, f"MP3 file appears to have invalid header: {blob_name}"
                elif file_ext == '.wav' and not header.startswith(b'RIFF'):
                    return False, f"WAV file appears to have invalid header: {blob_name}"
                elif file_ext == '.m4a' and not (header.startswith(b'ftyp') or header.startswith(b'M4A')):
                    return False, f"M4A file appears to have invalid header: {blob_name}"
                
        except Exception as e:
            return False, f"Error reading audio file: {str(e)}"
        
        return True, f"Audio file {blob_name} is valid ({file_size} bytes, {file_ext})"
        
    except Exception as e:
        return False, f"Validation error: {str(e)}"

def get_audio_file_info(blob_name: str, prefix: str = "") -> Dict[str, Any]:
    """
    Get detailed information about an audio file for debugging purposes.
    """
    try:
        local_file = download_blob_to_local_file(blob_name, prefix)
        
        if not local_file or not os.path.exists(local_file):
            return {"error": "File not found locally after download"}
        
        file_size = os.path.getsize(local_file)
        file_ext = os.path.splitext(blob_name.lower())[1]
        
        # Get file modification time
        mtime = os.path.getmtime(local_file)
        
        # Check if file is readable
        readable = os.access(local_file, os.R_OK)
        
        return {
            "filename": blob_name,
            "local_path": local_file,
            "extension": file_ext,
            "size_bytes": file_size,
            "size_mb": round(file_size / (1024 * 1024), 2),
            "modified": mtime,
            "readable": readable,
            "exists": True
        }
        
    except Exception as e:
        return {"error": str(e)}
