"""
Transcription Revision Service

This module provides services for creating enhanced, cleaned versions of transcriptions
in both Arabic Egyptian dialect and English while maintaining the original conversation
structure, formatting, and agent/customer dialogue flow.
"""

from services import azure_oai, azure_storage
from typing import Optional, Tuple
import re


class TranscriptionRevisionService:
    def __init__(self):
        """Initialize the transcription revision service."""
        pass
    
    def create_revised_arabic_transcription(self, original_transcription: str, call_analysis: Optional[dict] = None) -> str:
        """
        Create an enhanced, cleaned Arabic Egyptian conversation from the original transcription.
        
        Args:
            original_transcription: The original transcription text
            call_analysis: Optional call analysis data for context
            
        Returns:
            Enhanced Arabic transcription with corrected spelling and natural flow
        """
        
        # Extract context from call analysis if available
        context_info = ""
        if call_analysis and isinstance(call_analysis, dict):
            insights = call_analysis.get("Call Generated Insights", {})
            main_subject = insights.get("Main Subject", "")
            services = insights.get("Services", "")
            if main_subject:
                context_info += f"الموضوع الرئيسي للمكالمة: {main_subject}\n"
            if services:
                context_info += f"الخدمات المتعلقة: {services}\n"
        
        arabic_revision_prompt = f"""أنت مساعد متخصص في تحسين وتنقيح المحادثات العربية المصرية لمراكز خدمة العملاء.

مهمتك هي إنشاء نسخة محسنة ومنقحة من النسخة الأصلية للمحادثة مع الحفاظ على:
1. التسلسل الزمني الدقيق للمحادثة
2. تسميات المتحدثين (العميل: / الموظف:)
3. الطوابع الزمنية بنفس الصيغة [HH:MM:SS.mmm]
4. المعنى والسياق الأصلي للمحادثة
5. طبيعة الحوار بين العميل والموظف

التحسينات المطلوبة:
- تصحيح الأخطاء الإملائية والنحوية
- تحسين الوضوح والطلاقة مع الحفاظ على اللهجة المصرية الطبيعية
- تنسيق أفضل للجمل والعبارات
- إزالة التكرار غير الضروري
- تحسين التعبيرات المهنية للموظف
- الحفاظ على التعبيرات الطبيعية للعميل

{context_info}

إرشادات مهمة:
- لا تضيف معلومات جديدة لم تكن في النسخة الأصلية
- احتفظ بنفس عدد الأسطر تقريباً
- احتفظ بالطوابع الزمنية كما هي
- احتفظ بتسميات المتحدثين (العميل: / الموظف:)
- استخدم اللغة العربية المصرية الطبيعية والمهنية

النسخة الأصلية للمحادثة:
{original_transcription}

قم بإنشاء النسخة المحسنة والمنقحة:"""

        try:
            revised_arabic = azure_oai.call_llm(arabic_revision_prompt, "")
            
            # Clean up the response
            revised_arabic = revised_arabic.strip()
            
            # Ensure the format is maintained
            if not self._validate_transcription_format(revised_arabic):
                print("Warning: Revised Arabic transcription format validation failed")
                # Return original if revision failed
                return original_transcription
            
            return revised_arabic
            
        except Exception as e:
            print(f"Error creating revised Arabic transcription: {e}")
            return original_transcription
    
    def create_revised_english_transcription(self, original_transcription: str, call_analysis: Optional[dict] = None) -> str:
        """
        Create a cleaned English conversation from the original transcription.
        
        Args:
            original_transcription: The original transcription text
            call_analysis: Optional call analysis data for context
            
        Returns:
            Clean English transcription with corrected spelling and professional language
        """
        
        # Extract context from call analysis if available
        context_info = ""
        if call_analysis and isinstance(call_analysis, dict):
            insights = call_analysis.get("Call Generated Insights", {})
            main_subject = insights.get("Main Subject", "")
            services = insights.get("Services", "")
            if main_subject:
                context_info += f"Call Main Subject: {main_subject}\n"
            if services:
                context_info += f"Related Services: {services}\n"
        
        english_revision_prompt = f"""You are a professional call center conversation editor specializing in creating clean, professional English versions of customer service calls.

Your task is to create a revised, clean English version of the original conversation while maintaining:
1. The exact chronological order of the conversation
2. Speaker labels (Customer: / Agent:)
3. Timestamps in the same format [HH:MM:SS.mmm]
4. The original meaning and context of the conversation
5. The natural dialogue flow between customer and agent

Required improvements:
- Correct all spelling and grammatical errors
- Improve clarity and fluency with professional English
- Better sentence structure and phrasing
- Remove unnecessary repetition
- Enhance professional expressions for the agent
- Maintain natural customer expressions while correcting errors
- Translate Arabic content to natural English while preserving meaning

{context_info}

Important guidelines:
- Do not add new information that wasn't in the original
- Keep approximately the same number of lines
- Preserve timestamps exactly as they are
- Keep speaker labels (Customer: / Agent:)
- Use clear, professional English appropriate for call center context
- Maintain the conversational tone and customer service nature

Original conversation:
{original_transcription}

Create the revised, clean English version:"""

        try:
            revised_english = azure_oai.call_llm(english_revision_prompt, "")
            
            # Clean up the response
            revised_english = revised_english.strip()
            
            # Ensure the format is maintained
            if not self._validate_transcription_format(revised_english):
                print("Warning: Revised English transcription format validation failed")
                # Return a basic English version if revision failed
                return self._create_basic_english_version(original_transcription)
            
            return revised_english
            
        except Exception as e:
            print(f"Error creating revised English transcription: {e}")
            return self._create_basic_english_version(original_transcription)
    
    def _validate_transcription_format(self, transcription: str) -> bool:
        """
        Validate that the transcription maintains the expected format.
        
        Args:
            transcription: The transcription text to validate
            
        Returns:
            True if format is valid, False otherwise
        """
        if not transcription or len(transcription.strip()) == 0:
            return False
        
        lines = transcription.strip().split('\n')
        valid_lines = 0
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # Check for timestamp and speaker pattern
            # Expected format: [HH:MM:SS.mmm] Speaker: content
            pattern = r'^\[\d{2}:\d{2}:\d{2}\.\d{3}\]\s+(Agent|Customer|العميل|الموظف):\s+.+'
            if re.match(pattern, line, re.IGNORECASE):
                valid_lines += 1
        
        # At least 50% of non-empty lines should match the expected format
        non_empty_lines = len([l for l in lines if l.strip()])
        return valid_lines >= (non_empty_lines * 0.5) if non_empty_lines > 0 else False
    
    def _create_basic_english_version(self, original_transcription: str) -> str:
        """
        Create a basic English version as fallback.
        
        Args:
            original_transcription: The original transcription
            
        Returns:
            Basic English version with minimal changes
        """
        try:
            # Simple fallback: just clean up obvious issues and translate speaker labels
            lines = original_transcription.strip().split('\n')
            english_lines = []
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                # Replace Arabic speaker labels with English
                line = re.sub(r'العميل:', 'Customer:', line)
                line = re.sub(r'الموظف:', 'Agent:', line)
                
                english_lines.append(line)
            
            return '\n'.join(english_lines)
            
        except Exception as e:
            print(f"Error creating basic English version: {e}")
            return original_transcription
    
    def process_single_transcription(self, call_id: str, force_regenerate: bool = False) -> Tuple[bool, bool, str]:
        """
        Process a single transcription to create both Arabic and English revised versions.
        
        Args:
            call_id: The call ID (filename without extension)
            force_regenerate: Whether to regenerate even if versions already exist
            
        Returns:
            Tuple of (arabic_success, english_success, message)
        """
        try:
            # Check if original transcription exists
            original_transcription = azure_storage.read_transcription(f"{call_id}.txt")
            if not original_transcription:
                return False, False, f"Original transcription not found for {call_id}"
            
            # Check if revised versions already exist
            arabic_exists = azure_storage.revised_arabic_transcription_already_exists(call_id)
            english_exists = azure_storage.revised_english_transcription_already_exists(call_id)
            
            if not force_regenerate and arabic_exists and english_exists:
                return True, True, f"Revised transcriptions already exist for {call_id}"
            
            # Try to get call analysis for context
            call_analysis = None
            try:
                call_analysis = azure_storage.read_llm_analysis("persona", f"{call_id}.json")
            except:
                pass  # Analysis not available, continue without it
            
            arabic_success = False
            english_success = False
            
            # Create revised Arabic transcription
            if force_regenerate or not arabic_exists:
                print(f"Creating revised Arabic transcription for {call_id}...")
                revised_arabic = self.create_revised_arabic_transcription(original_transcription, call_analysis)
                try:
                    azure_storage.upload_revised_arabic_transcription_to_blob(call_id, revised_arabic)
                    arabic_success = True
                    print(f"✅ Arabic revision saved for {call_id}")
                except Exception as e:
                    print(f"Error saving Arabic revision for {call_id}: {e}")
            else:
                arabic_success = True
            
            # Create revised English transcription
            if force_regenerate or not english_exists:
                print(f"Creating revised English transcription for {call_id}...")
                revised_english = self.create_revised_english_transcription(original_transcription, call_analysis)
                try:
                    azure_storage.upload_revised_english_transcription_to_blob(call_id, revised_english)
                    english_success = True
                    print(f"✅ English revision saved for {call_id}")
                except Exception as e:
                    print(f"Error saving English revision for {call_id}: {e}")
            else:
                english_success = True
            
            if arabic_success and english_success:
                return True, True, f"Successfully processed revisions for {call_id}"
            elif arabic_success:
                return True, False, f"Arabic revision successful, English revision failed for {call_id}"
            elif english_success:
                return False, True, f"English revision successful, Arabic revision failed for {call_id}"
            else:
                return False, False, f"Both revisions failed for {call_id}"
                
        except Exception as e:
            return False, False, f"Error processing transcription {call_id}: {str(e)}"
    
    def process_all_transcriptions(self, force_regenerate: bool = False) -> dict:
        """
        Process all transcriptions in the blob container to create revised versions.
        
        Args:
            force_regenerate: Whether to regenerate even if versions already exist
            
        Returns:
            Dictionary with processing results
        """
        print("Starting batch processing of all transcriptions...")
        
        # Get all transcription files
        transcription_files = azure_storage.list_blobs(azure_storage.TRANSCRIPTION_FOLDER)
        call_ids = [f.replace('.txt', '') for f in transcription_files if f.endswith('.txt')]
        
        if not call_ids:
            return {
                "status": "error",
                "message": "No transcriptions found",
                "processed": 0,
                "arabic_success": 0,
                "english_success": 0,
                "errors": []
            }
        
        print(f"Found {len(call_ids)} transcriptions to process")
        
        results = {
            "status": "completed",
            "message": f"Processed {len(call_ids)} transcriptions",
            "processed": 0,
            "arabic_success": 0,
            "english_success": 0,
            "errors": []
        }
        
        for i, call_id in enumerate(call_ids, 1):
            print(f"Processing {i}/{len(call_ids)}: {call_id}")
            
            arabic_success, english_success, message = self.process_single_transcription(
                call_id, force_regenerate
            )
            
            results["processed"] += 1
            if arabic_success:
                results["arabic_success"] += 1
            if english_success:
                results["english_success"] += 1
            
            if not arabic_success or not english_success:
                results["errors"].append({
                    "call_id": call_id,
                    "message": message,
                    "arabic_success": arabic_success,
                    "english_success": english_success
                })
        
        print(f"Batch processing complete:")
        print(f"  - Total processed: {results['processed']}")
        print(f"  - Arabic successes: {results['arabic_success']}")
        print(f"  - English successes: {results['english_success']}")
        print(f"  - Errors: {len(results['errors'])}")
        
        return results


# Global instance
revision_service = TranscriptionRevisionService()
