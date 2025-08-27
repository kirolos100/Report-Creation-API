import os
import threading
from dotenv import load_dotenv

import azure.cognitiveservices.speech as speechsdk

load_dotenv()


def _ticks_to_timestamp(ticks: int) -> str:
    # Azure Speech offsets/durations are in 100-nanosecond units (ticks)
    total_ms = int(round(ticks / 10000))
    hours = total_ms // 3600000
    minutes = (total_ms % 3600000) // 60000
    seconds = (total_ms % 60000) // 1000
    millis = total_ms % 1000
    return f"[{hours:02d}:{minutes:02d}:{seconds:02d}.{millis:03d}]"


def transcribe_with_speech_sdk(audio_file_path: str) -> str:
    """
    Transcribe using Azure Cognitive Services Speech SDK with word-level timestamps.
    Returns a timestamped plain-text transcript.
    """
    speech_key = "5bVGgxC4rjSjBhKgngZDLdSm5cLiNida4vXJ8vEIWQi608yOQj1GJQQJ99BGACF24PCXJ3w3AAAYACOGnyyd"
    speech_region = "uaenorth"
    if not speech_key or not speech_region:
        raise ValueError("AZURE_SPEECH_KEY and AZURE_SPEECH_REGION must be set for Speech SDK transcription.")

    # Validate audio file exists and is readable
    if not os.path.exists(audio_file_path):
        raise FileNotFoundError(f"Audio file not found: {audio_file_path}")
    
    if not os.access(audio_file_path, os.R_OK):
        raise PermissionError(f"Cannot read audio file: {audio_file_path}")

    try:
        speech_config = speechsdk.SpeechConfig(subscription=speech_key, region=speech_region)
        speech_config.output_format = speechsdk.OutputFormat.Detailed
        speech_config.set_property(
            property_id=speechsdk.PropertyId.SpeechServiceResponse_RequestWordLevelTimestamps,
            value="true",
        )
        
        # Add additional configuration for better compatibility
        speech_config.set_property(
            property_id=speechsdk.PropertyId.SpeechServiceConnection_InitialSilenceTimeoutMs,
            value="5000"
        )
        speech_config.set_property(
            property_id=speechsdk.PropertyId.SpeechServiceConnection_EndSilenceTimeoutMs,
            value="2000"
        )

        audio_config = speechsdk.audio.AudioConfig(filename=audio_file_path)
        recognizer = speechsdk.SpeechRecognizer(speech_config=speech_config, audio_config=audio_config)

        done = threading.Event()
        lines = []
        error_occurred = False
        error_message = ""

        def handle_recognized(evt: speechsdk.SpeechRecognitionEventArgs):
            if not evt or not evt.result:
                return
            result = evt.result
            # Detailed output: access the first NBest alternative for words
            try:
                json = result.json
            except Exception:
                json = None

            # Fallback to result.text if parsing detailed words is not available
            timestamp = "[00:00:00.000]"
            text = result.text or ""

            # Try to parse NBest words via SDK object if available
            try:
                # In detailed mode, result.best() returns the best alternative with words
                best = result.best()
                if best and getattr(best, "words", None):
                    words = best.words
                    if len(words) > 0 and getattr(words[0], "offset", None) is not None:
                        timestamp = _ticks_to_timestamp(words[0].offset)
            except Exception:
                pass

            if text:
                lines.append(f"{timestamp} {text.strip()}")

        def handle_canceled(evt: speechsdk.SpeechRecognitionCanceledEventArgs):
            nonlocal error_occurred, error_message
            error_occurred = True
            error_message = f"Recognition canceled: {evt.reason}"
            if evt.reason == speechsdk.CancellationReason.Error:
                error_message += f" - Error code: {evt.error_code}, Error details: {evt.error_details}"
            done.set()

        def stop_cb(evt):
            done.set()

        recognizer.recognized.connect(handle_recognized)
        recognizer.canceled.connect(handle_canceled)
        recognizer.session_stopped.connect(stop_cb)

        # Set a timeout for the recognition process
        recognizer.start_continuous_recognition()
        
        # Wait for completion with timeout (5 minutes max)
        if not done.wait(timeout=300):
            recognizer.stop_continuous_recognition()
            raise RuntimeError("Speech recognition timed out after 5 minutes")
        
        recognizer.stop_continuous_recognition()

        if error_occurred:
            raise RuntimeError(error_message)

        if not lines:
            raise RuntimeError("No transcription generated - audio may be silent or corrupted")

        return "\n".join(lines)

    except Exception as e:
        # Provide more specific error messages
        if "SPXERR_INVALID_HEADER" in str(e):
            raise RuntimeError(f"Invalid audio file header: {audio_file_path} may be corrupted or in an unsupported format")
        elif "SPXERR_FILE_IO" in str(e):
            raise RuntimeError(f"Audio file I/O error: {audio_file_path} may be corrupted or inaccessible")
        elif "SPXERR_INVALID_ARG" in str(e):
            raise RuntimeError(f"Invalid audio file argument: {audio_file_path} format not supported")
        else:
            raise RuntimeError(f"Speech SDK transcription failed for {audio_file_path}: {str(e)}")


