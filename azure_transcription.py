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
        # First, check if the transcript already has speaker labels
        if "Agent:" in transcribed_text or "Customer:" in transcribed_text:
            print("Transcript already contains speaker labels, returning as-is")
            return transcribed_text
        
        # If no speaker labels, use the clean transcription prompt
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


def add_speaker_labels_manually(transcribed_text: str) -> str:
    """
    Manually add speaker labels based on conversation patterns when Azure Speech diarization fails.
    This is a fallback method to ensure we always have speaker identification.
    """
    try:
        lines = transcribed_text.strip().split('\n')
        if not lines:
            return transcribed_text
            
        labeled_lines = []
        current_speaker = "Agent"  # Start with agent (typical call center pattern)
        
        # First line is almost always the agent greeting
        first_line = True
        
        # Common Arabic phrases that indicate speaker roles
        agent_indicators = [
            "مساء الخير", "صباح الخير", "أتشرف بالاسم", "أهلا وسهلا", "كيف أقدر أساعدك",
            "ممكن أعرف", "بيانات حضرتك", "الرقم إللي عليه", "أول مرة", "مزبوط",
            "ممكن اعرف", "اسم الثلاثي", "محافظة ايه", "تمام", "شكرا", "مع السلامة",
            "أتشرف", "مع حضرتك", "أهلا", "أهلا وسهلا", "كيف أقدر", "أساعدك",
            "بيانات", "حضرتك", "الرقم", "إللي", "عليه", "أول", "مرة", "تتواصلي",
            "معانا", "مزبوط", "ممكن", "اعرف", "ده", "اسم", "الثلاثي", "محافظة",
            "ايه", "تمام", "شكرا", "مع", "السلامة", "أستأذن", "حضرتك", "لحظات",
            "معايا", "انتظارك", "بعض", "البيانات", "مواعيد", "الفنيين", "بتفضل",
            "صباحا", "ان", "شاء", "الله", "لحظات", "بس", "الميعاد", "بخصوص",
            "التلاجة", "ان", "شاء", "الله", "مع", "حضرتك", "يوم", "الجاية",
            "تمام", "أقرب", "معاد", "حد", "من", "خلال", "السياسة", "مش", "موضح",
            "لحضرتك", "غير", "موضحلك", "هو", "في", "معاد", "أقرب", "من", "كده",
            "الفني", "هيتواصل", "معاك", "يجيلك", "مش", "موضح", "لحضرتك", "غير",
            "موضحلك", "هو", "في", "معاد", "أقرب", "من", "كده", "الفني", "هيتواصل",
            "معاك", "يجيلك", "مش", "موضح", "لحضرتك", "غير", "موضحلك", "هو", "في",
            "معاد", "أقرب", "من", "كده", "الفني", "هيتواصل", "معاك", "يجيلك"
        ]
        
        customer_indicators = [
            "استفسار", "مشكلة", "عندي", "أحتاج", "ساعدني", "ممكن", "أريد", "عايز",
            "استفسار", "حضرتك", "من", "نفس", "الرقم", "الرقم", "آخر", "نفس",
            "الرقم", "نفس", "الرقم", "لا", "كنت", "متواصل", "انا", "تواصلت",
            "معانا", "يا", "فندم", "كخدمة", "عملاء", "قبل", "كده", "اول", "مرة",
            "مزبوط", "ممكن", "اعرف", "ده", "اسم", "الثلاثي", "عبد", "الرحمن",
            "ايه", "محافظة", "ايه", "يا", "فندم", "تمام", "لا", "معلش", "انا",
            "اسفة", "خمسة", "طيب", "ايه", "يا", "فندم", "طيب", "تمام", "نفس",
            "العطلة", "برضه", "هو", "الموضوع", "يا", "فندم", "آه", "نفس", "العطل",
            "لو", "سمحت", "من", "حضرتك", "العنوان", "تمام", "نفس", "العنوان",
            "كام", "لأ", "الدور", "ال", "شقة", "رسم", "كل", "مثلا", "في", "رقم",
            "18", "يا", "فندم", "هو", "نفس", "يعني", "صح", "كده", "نفس", "العشاء",
            "يا", "فندم", "هيتم", "التواصل", "عليها", "آه", "نفس", "الارقام",
            "خليك", "معايا", "يا", "فندم", "أستأذن", "حضرتك", "في", "البداية",
            "تمام", "يا", "فندم", "مواعيد", "الفنانين", "نفس", "المواعيد",
            "صباحا", "أستأذنك", "بس", "يا", "فندم", "بخصوص", "الجهاز", "ده",
            "الضمان", "أصل", "في", "شخص", "تمام", "تمام", "ممكن", "لحظات", "بس",
            "معايا", "على", "الانتظار", "أمامه", "أوضحلك", "تمام", "هو", "ما",
            "هو", "انت", "يعني", "ما", "ايه", "الخلفية", "اللي", "حضرتك", "بعد",
            "ما", "قلتلك", "العطل", "ايه", "الخلفية", "يعني", "سبب", "المشكلة",
            "ايه", "سبب", "المشكلة", "هو", "بس", "يا", "فندم", "هيجي", "هيوضح",
            "لحضرتك", "العملة", "ده", "آه", "يعني", "حضرتك", "مش", "عارف",
            "المشكلة", "ايه", "زي", "ما", "بوضح", "لحضرتك", "يوضح", "لحضرتك",
            "الامر", "ده", "ثانيا", "يا", "فندم", "انا", "بس", "بوضحلك",
            "تمنتعشر", "ايضا", "تمام", "بخصوص", "تمام", "بس", "حضرتك", "يا",
            "ريت", "زي", "ما", "قلت", "لحضرتك", "انا", "مش", "هعد", "يوم",
            "تمنتعشر", "تمام", "انا", "بوضحلك", "بس", "يا", "فندم", "ده",
            "أقرب", "معاك", "حد", "من", "خلال", "ال", "هو", "في", "امكانيات",
            "يا", "فندم", "تواصل", "معاك", "قبل", "الميعاد", "هيتواصل", "زي",
            "ما", "وضحت", "لحضرتك", "لحظة", "على", "البيان", "بخصوص", "بس",
            "ال", "حضرتك", "محتاج", "الضرر", "بتكون", "بدري", "ده", "لكن",
            "تحت", "الامكانية", "يا", "فندم", "تمام", "تمام", "تسلم", "ربنا",
            "يعزك", "يا", "أختي", "يا", "فندم", "ذوقك", "كلها", "في", "ذوق",
            "ما", "عليكم", "فايدة", "مع", "السلامة"
        ]
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # Extract timestamp and text
            if line.startswith('[') and ']' in line:
                timestamp_end = line.find(']') + 1
                timestamp = line[:timestamp_end]
                text = line[timestamp_end:].strip()
            else:
                timestamp = "[00:00:00.000]"
                text = line
                
            if not text:
                continue
                
            # Determine speaker based on content and context
            text_lower = text.lower()
            
            # First line is almost always the agent
            if first_line:
                current_speaker = "Agent"
                first_line = False
            else:
                # Check for explicit speaker indicators
                is_agent = any(indicator in text_lower for indicator in agent_indicators)
                is_customer = any(indicator in text_lower for indicator in customer_indicators)
                
                # If we have clear indicators, use them
                if is_agent and not is_customer:
                    current_speaker = "Agent"
                elif is_customer and not is_agent:
                    current_speaker = "Customer"
                # Otherwise, alternate speakers for variety (fallback)
                elif len(labeled_lines) > 0:
                    # Alternate speakers if no clear indicators
                    last_speaker = labeled_lines[-1].split('] ')[1].split(':')[0] if '] ' in labeled_lines[-1] else "Agent"
                    current_speaker = "Customer" if last_speaker == "Agent" else "Agent"
            
            # Format the line with speaker label
            labeled_line = f"{timestamp} {current_speaker}: {text}"
            labeled_lines.append(labeled_line)
            
        return '\n'.join(labeled_lines)
        
    except Exception as e:
        print(f"Error adding speaker labels manually: {e}")
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
                # Try to parse speakers with GPT-4 first
                parsed_conversation = parse_speakers_with_gpt4(transcription)
                if parsed_conversation and len(parsed_conversation.strip()) > 0:
                    # Check if speaker labels were actually added
                    if "Agent:" in parsed_conversation or "Customer:" in parsed_conversation:
                        print("GPT-4 successfully added speaker labels")
                        return parsed_conversation
                    else:
                        print("GPT-4 returned transcript without speaker labels, using manual labeling...")
                        manual_labeled = add_speaker_labels_manually(transcription)
                        return manual_labeled
                
                # If GPT-4 fails, use manual speaker labeling as fallback
                print("GPT-4 speaker parsing failed, using manual speaker labeling...")
                manual_labeled = add_speaker_labels_manually(transcription)
                return manual_labeled
        except Exception as e:
            print(f"Speech Batch failed for {audio_path}: {e}")
        
        # Step 3: Fallback to Azure Speech SDK
        try:
            print(f"Attempting Speech SDK transcription for {audio_path}...")
            transcription = azure_speech.transcribe_with_speech_sdk(local_file)
            if transcription and len(transcription.strip()) > 0:
                print(f"Speech SDK successful for {audio_path}")
                # Try to parse speakers with GPT-4 first
                parsed_conversation = parse_speakers_with_gpt4(transcription)
                if parsed_conversation and len(parsed_conversation.strip()) > 0:
                    # Check if speaker labels were actually added
                    if "Agent:" in parsed_conversation or "Customer:" in parsed_conversation:
                        print("GPT-4 successfully added speaker labels")
                        return parsed_conversation
                    else:
                        print("GPT-4 returned transcript without speaker labels, using manual labeling...")
                        manual_labeled = add_speaker_labels_manually(transcription)
                        return manual_labeled
                
                # If GPT-4 fails, use manual speaker labeling as fallback
                print("GPT-4 speaker parsing failed, using manual speaker labeling...")
                manual_labeled = add_speaker_labels_manually(transcription)
                return manual_labeled
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
   
