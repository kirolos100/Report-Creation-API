#!/usr/bin/env python3
"""
Generate Revised Transcriptions for All Existing Calls

This script will process all existing transcriptions in the blob container
and generate both Arabic and English revised versions.

Usage:
    python generate_all_revised_transcriptions.py [--force]
    
Options:
    --force    Force regeneration even if revised versions already exist
"""

import sys
import argparse
from transcription_revision_service import revision_service
from services import azure_storage


def main():
    parser = argparse.ArgumentParser(description='Generate revised transcriptions for all existing calls')
    parser.add_argument('--force', action='store_true', 
                       help='Force regeneration even if revised versions already exist')
    
    args = parser.parse_args()
    
    print("🚀 Starting batch generation of revised transcriptions...")
    print("=" * 60)
    
    # Get current status
    original_count = len(azure_storage.list_blobs(azure_storage.TRANSCRIPTION_FOLDER))
    arabic_count = len(azure_storage.list_blobs(azure_storage.REVISED_ARABIC_FOLDER))
    english_count = len(azure_storage.list_blobs(azure_storage.REVISED_ENGLISH_FOLDER))
    
    print(f"📊 Current Status:")
    print(f"  Original transcriptions: {original_count}")
    print(f"  Revised Arabic: {arabic_count}")
    print(f"  Revised English: {english_count}")
    print()
    
    if args.force:
        print("⚠️  FORCE MODE: Will regenerate all revised transcriptions")
    else:
        print("📝 NORMAL MODE: Will skip existing revised transcriptions")
    
    print()
    
    # Confirm before proceeding
    if original_count > 10:
        response = input(f"⚠️  About to process {original_count} transcriptions. Continue? (y/N): ")
        if response.lower() != 'y':
            print("❌ Operation cancelled by user")
            sys.exit(1)
    
    # Process all transcriptions
    print("🔄 Processing transcriptions...")
    results = revision_service.process_all_transcriptions(force_regenerate=args.force)
    
    print()
    print("=" * 60)
    print("✅ BATCH PROCESSING COMPLETE!")
    print("=" * 60)
    print(f"📊 Results:")
    print(f"  Total processed: {results['processed']}")
    print(f"  Arabic successes: {results['arabic_success']}")
    print(f"  English successes: {results['english_success']}")
    print(f"  Errors: {len(results['errors'])}")
    
    if results['errors']:
        print()
        print("❌ Errors encountered:")
        for error in results['errors'][:5]:  # Show first 5 errors
            print(f"  - {error['call_id']}: {error['message']}")
        
        if len(results['errors']) > 5:
            print(f"  ... and {len(results['errors']) - 5} more errors")
    
    # Final status
    final_arabic_count = len(azure_storage.list_blobs(azure_storage.REVISED_ARABIC_FOLDER))
    final_english_count = len(azure_storage.list_blobs(azure_storage.REVISED_ENGLISH_FOLDER))
    
    print()
    print(f"📈 Final Status:")
    print(f"  Original transcriptions: {original_count}")
    print(f"  Revised Arabic: {final_arabic_count} (+{final_arabic_count - arabic_count})")
    print(f"  Revised English: {final_english_count} (+{final_english_count - english_count})")
    
    print()
    print("🎉 All revised transcriptions are now available for the CallDetails page!")
    print("   Users can now see 3 tabs: Original, Revised Arabic, and Revised English")


if __name__ == "__main__":
    main()
