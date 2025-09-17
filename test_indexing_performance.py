#!/usr/bin/env python3
"""
Test script to verify the improved Azure Search indexing performance.

This script helps test and validate the optimized indexing implementation
to ensure documents are indexed quickly and available immediately.
"""

import time
import json
from datetime import datetime
from services import azure_search

def test_indexing_performance():
    """Test the indexing performance improvements."""
    
    print("Azure Search Indexing Performance Test")
    print("=" * 50)
    
    index_name = "marketing_sentiment_details"
    
    # Check if index exists
    if not azure_search.index_exists(index_name):
        print(f"❌ Index '{index_name}' does not exist")
        print("Please upload some audio files first to create the index")
        return
    
    # Get current document count
    initial_count = azure_search.get_index_document_count(index_name)
    print(f"📊 Current index document count: {initial_count}")
    
    # Create a test document
    test_doc_id = f"test_doc_{int(time.time())}"
    test_document = {
        "id": test_doc_id,
        "call_id": test_doc_id,
        "name": None,  # Avoid the problematic name field
        "summary": "This is a test document for performance testing",
        "Call Generated Insights": {
            "Main Subject": "Performance Test",
            "Customer Sentiment": "Neutral",
            "Call Categorization": "Test",
            "Resolution Status": "resolved",
            "Services": "Testing Service",
            "Call Outcome": "Test completed successfully",
            "Agent Attitude": "Professional and Efficient"
        },
        "Customer Service Metrics": {
            "FCR": {"score": True, "explanation": "Test resolved on first contact"},
            "Talk time": 120,
            "Hold time": 5
        },
        "sentiment": {"score": 3, "explanation": "Neutral test sentiment"},
        "main_issues": ["Performance testing"],
        "resolution": "Test document created successfully",
        "agent_professionalism": "Professional"
    }
    
    print(f"\n🧪 Testing indexing performance with document ID: {test_doc_id}")
    
    # Test the optimized indexing method
    print("\n1. Testing optimized indexing method...")
    start_time = time.time()
    
    try:
        message, success, indexed_doc_ids = azure_search.load_json_into_azure_search_optimized(
            index_name, [test_document], wait_for_completion=True
        )
        
        end_time = time.time()
        indexing_duration = end_time - start_time
        
        print(f"⏱️  Indexing completed in {indexing_duration:.2f} seconds")
        print(f"📝 Result: {message}")
        print(f"✅ Success: {success}")
        print(f"📄 Indexed documents: {indexed_doc_ids}")
        
        if success and test_doc_id in indexed_doc_ids:
            print(f"🎉 Document {test_doc_id} successfully indexed and verified!")
            
            # Verify document exists
            doc_exists = azure_search.document_exists_in_index(index_name, test_doc_id)
            print(f"🔍 Document existence verification: {doc_exists}")
            
            # Check new document count
            new_count = azure_search.get_index_document_count(index_name)
            print(f"📊 New index document count: {new_count}")
            
            if new_count > initial_count:
                print(f"📈 Successfully added {new_count - initial_count} document(s)")
            else:
                print(f"🔄 Document may have updated existing entry")
                
        else:
            print(f"⚠️ Document indexing had issues")
            
    except Exception as e:
        print(f"❌ Error during optimized indexing test: {e}")
        import traceback
        traceback.print_exc()
    
    # Test document retrieval
    print(f"\n2. Testing document retrieval...")
    try:
        doc_exists = azure_search.document_exists_in_index(index_name, test_doc_id)
        if doc_exists:
            print(f"✅ Document {test_doc_id} is retrievable from index")
        else:
            print(f"❌ Document {test_doc_id} not found in index")
    except Exception as e:
        print(f"❌ Error during document retrieval test: {e}")
    
    # Performance summary
    print(f"\n📈 Performance Summary:")
    print(f"  - Index: {index_name}")
    print(f"  - Initial document count: {initial_count}")
    print(f"  - Test document ID: {test_doc_id}")
    print(f"  - Indexing duration: {indexing_duration:.2f} seconds")
    print(f"  - Success: {success}")
    
    # Cleanup - remove test document
    print(f"\n🧹 Cleaning up test document...")
    try:
        cleanup_success, cleanup_message = azure_search.delete_document_from_index(index_name, test_doc_id)
        if cleanup_success:
            print(f"✅ Test document cleaned up successfully")
        else:
            print(f"⚠️ Cleanup warning: {cleanup_message}")
    except Exception as e:
        print(f"⚠️ Cleanup error: {e}")
    
    print(f"\n" + "=" * 50)
    print("Performance test completed!")


def test_multiple_documents():
    """Test indexing multiple documents for batch performance."""
    
    print("\nBatch Indexing Performance Test")
    print("-" * 40)
    
    index_name = "marketing_sentiment_details"
    
    if not azure_search.index_exists(index_name):
        print(f"❌ Index '{index_name}' does not exist")
        return
    
    # Create multiple test documents
    num_docs = 3
    test_docs = []
    test_doc_ids = []
    
    for i in range(num_docs):
        doc_id = f"batch_test_{int(time.time())}_{i}"
        test_doc_ids.append(doc_id)
        
        test_doc = {
            "id": doc_id,
            "call_id": doc_id,
            "summary": f"Batch test document {i+1} for performance testing",
            "Call Generated Insights": {
                "Main Subject": f"Batch Performance Test {i+1}",
                "Customer Sentiment": "Neutral",
                "Call Categorization": "Test",
                "Resolution Status": "resolved"
            },
            "sentiment": {"score": 3, "explanation": "Neutral test sentiment"},
            "main_issues": [f"Batch testing {i+1}"]
        }
        test_docs.append(test_doc)
    
    print(f"🧪 Testing batch indexing of {num_docs} documents...")
    
    start_time = time.time()
    try:
        message, success, indexed_doc_ids = azure_search.load_json_into_azure_search_optimized(
            index_name, test_docs, wait_for_completion=True
        )
        
        end_time = time.time()
        batch_duration = end_time - start_time
        
        print(f"⏱️  Batch indexing completed in {batch_duration:.2f} seconds")
        print(f"📝 Result: {message}")
        print(f"✅ Success: {success}")
        print(f"📄 Successfully indexed: {len(indexed_doc_ids)}/{num_docs} documents")
        
        # Cleanup batch test documents
        print(f"🧹 Cleaning up {len(test_doc_ids)} test documents...")
        for doc_id in test_doc_ids:
            try:
                azure_search.delete_document_from_index(index_name, doc_id)
            except:
                pass
        
        print(f"✅ Batch test completed - Average: {batch_duration/num_docs:.2f} seconds per document")
        
    except Exception as e:
        print(f"❌ Batch indexing test failed: {e}")


if __name__ == "__main__":
    # Run performance tests
    test_indexing_performance()
    test_multiple_documents()
