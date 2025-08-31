
from services import azure_oai
from services import azure_speech
from services import azure_speech_batch
from dotenv import load_dotenv
from services import azure_storage
import os
import mimetypes

load_dotenv()

def validate_audio_file(audio_path: str) -> tuple[bool, str]:
    """
    Validate audio file before transcription attempts.
    Returns (is_valid, error_message)
    """
    try:
        # Check file extension
        audio_exts = ('.mp3', '.wav', '.m4a', '.mp4', '.aac', '.ogg')
        file_ext = os.path.splitext(audio_path.lower())[1]
        if file_ext not in audio_exts:
            return False, f"Unsupported audio format: {file_ext}. Supported: {', '.join(audio_exts)}"
        
        # Check if file exists in blob storage
        try:
            local_file = azure_storage.download_audio_to_local_file(audio_path)
            if not os.path.exists(local_file):
                return False, f"Audio file not found in storage: {audio_path}"
            
            # Check file size (minimum 1KB, maximum 100MB)
            file_size = os.path.getsize(local_file)
            if file_size < 1024:
                return False, f"Audio file too small: {file_size} bytes (minimum 1KB required)"
            if file_size > 100 * 1024 * 1024:
                return False, f"Audio file too large: {file_size} bytes (maximum 100MB allowed)"
                
            return True, ""
        except Exception as e:
            return False, f"Error accessing audio file: {str(e)}"
            
    except Exception as e:
        return False, f"Validation error: {str(e)}"

def parse_speakers_with_gpt4(transcribed_text: str) -> str:
    try:
        new_transcription = azure_oai.call_llm('./misc/clean_transcription.txt', transcribed_text)
        # Defensive fallback: if the model cannot diarize, keep original transcript
        output_text = (new_transcription or "").strip()
        if not output_text:
            return transcribed_text
        lower = output_text.lower()
        if "does not contain enough information" in lower or len(output_text.splitlines()) < 2:
            return transcribed_text
        return output_text
    except Exception as e:
        print(f"Error cleaning transcription with 4o: {e}")
        return transcribed_text

def transcribe_audio(audio_path: str):
    """
    Transcribe audio using Azure Speech services with improved error handling and validation.
    """
    try:
        # Step 1: Validate audio file
        is_valid, error_msg = validate_audio_file(audio_path)
        if not is_valid:
            return f"Audio validation failed: {error_msg}"
        
        audio_path = audio_path.replace(" ", "_")
        local_file = azure_storage.download_audio_to_local_file(audio_path)
        
        # Step 2: Try Azure Speech Batch first (better for longer audio files)
        try:
            print(f"Attempting Speech Batch transcription for {audio_path}...")
            sas_url = azure_storage.get_audio_blob_sas_url(audio_path)
            transcription = azure_speech_batch.transcribe_with_speech_batch(sas_url)
            if transcription and len(transcription.strip()) > 0:
                print(f"Speech Batch successful for {audio_path}")
                # Parse speakers with GPT-4 for better conversation structure
                parsed_conversation = parse_speakers_with_gpt4(transcription)
                if parsed_conversation and len(parsed_conversation.strip()) > 0:
                    return parsed_conversation
                return transcription
        except Exception as e:
            print(f"Speech Batch failed for {audio_path}: {e}")
        
        # Step 3: Fallback to Azure Speech SDK
        try:
            print(f"Attempting Speech SDK transcription for {audio_path}...")
            transcription = azure_speech.transcribe_with_speech_sdk(local_file)
            if transcription and len(transcription.strip()) > 0:
                print(f"Speech SDK successful for {audio_path}")
                # Parse speakers with GPT-4 for better conversation structure
                parsed_conversation = parse_speakers_with_gpt4(transcription)
                if parsed_conversation and len(parsed_conversation.strip()) > 0:
                    return parsed_conversation
                return transcription
        except Exception as e:
            print(f"Speech SDK failed for {audio_path}: {e}")
        
        # Step 4: If both Azure Speech methods fail, provide detailed error
        error_details = f"All Azure Speech transcription methods failed for {audio_path}. "
        error_details += "This may be due to: 1) Corrupted audio file, 2) Unsupported audio format, "
        error_details += "3) Audio file too short or too long, 4) Network connectivity issues. "
        error_details += "Please check the audio file and try again."
        
        return error_details
          
    except Exception as e:
        print(f"Critical error transcribing {audio_path}: {e}")
        return f"Critical transcription error for {audio_path}: {str(e)}"
   
