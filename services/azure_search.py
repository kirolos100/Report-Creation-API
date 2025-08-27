import os
from services import azure_oai
import json
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
from azure.core.credentials import AzureKeyCredential

import re

from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    SimpleField,
    SearchableField,
    SearchFieldDataType,
    SearchField,
    VectorSearch,
    HnswAlgorithmConfiguration,
    VectorSearchProfile,
    SemanticConfiguration,
    SemanticPrioritizedFields,
    SemanticField,
    SemanticSearch,
    SearchIndex,
)

load_dotenv()

# Prefer API key if provided; otherwise fall back to AAD
AZURE_SEARCH_ADMIN_KEY = os.getenv("AZURE_SEARCH_ADMIN_KEY") or os.getenv("AZURE_SEARCH_API_KEY") or os.getenv("AZURE_SEARCH_KEY")
AZURE_SEARCH_QUERY_KEY = os.getenv("AZURE_SEARCH_QUERY_KEY")  # optional, for query-only
azure_credentials = DefaultAzureCredential()

AZURE_SEARCH_ENDPOINT = os.getenv("AZURE_SEARCH_ENDPOINT")
if not AZURE_SEARCH_ENDPOINT:
    raise ValueError("Please provide a valid Azure Search endpoint.")

def _get_index_credential():
    # Use admin key for index management if available
    if AZURE_SEARCH_ADMIN_KEY:
        return AzureKeyCredential(AZURE_SEARCH_ADMIN_KEY)
    return azure_credentials

def _get_query_credential():
    # Prefer a query key if provided, else admin key, else AAD
    if AZURE_SEARCH_QUERY_KEY:
        return AzureKeyCredential(AZURE_SEARCH_QUERY_KEY)
    if AZURE_SEARCH_ADMIN_KEY:
        return AzureKeyCredential(AZURE_SEARCH_ADMIN_KEY)
    return azure_credentials

def get_search_index_client():
    search_index_client = SearchIndexClient(
        endpoint=AZURE_SEARCH_ENDPOINT,
        credential=_get_index_credential(),
    )
    return search_index_client

def get_search_client(index_name):
    search_client = SearchClient(
        endpoint=AZURE_SEARCH_ENDPOINT,
        index_name=index_name,
        credential=_get_query_credential(),
    )
    return search_client

# ------------------------------------------------------------------------------
# Helpers to inspect index vector dimensions
# ------------------------------------------------------------------------------
def get_index_vector_dim(index_name: str):
    try:
        existing = get_search_index_client().get_index(index_name)
        for f in existing.fields:
            if getattr(f, "name", None) == "contentVector":
                return getattr(f, "vector_search_dimensions", None)
        return None
    except Exception:
        return None

def is_index_dim_mismatch(index_name: str) -> bool:
    dim = get_index_vector_dim(index_name)
    return dim is not None and dim != azure_oai.EMBEDDING_DIM

# ------------------------------------------------------------------------------
# 2) Helpers to Flatten JSON and Infer Fields
# ------------------------------------------------------------------------------
def flatten_json(nested_json, parent_key="", sep="."):
    """
    Flatten JSON if there's only a single level of nesting.
    Example:
       {
         "sentiment": "great",
         "score": 2,
         "explanation": {
              "reason": "agent was helpful",
              "feedback": "awesome"
          }
       }
    -> {
         "sentiment": "great",
         "score": 2,
         "explanation.reason": "agent was helpful",
         "explanation.feedback": "awesome"
       }
    """
    items = []
    for k, v in nested_json.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_json(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def normalize_field_name(name: str) -> str:
    # Replace any character that is not a letter, digit, or underscore with an underscore.
    normalized = re.sub(r'[^A-Za-z0-9_]', '_', name)
    # Ensure the field name starts with a letter; if not, prefix it with "f_"
    if not re.match(r'^[A-Za-z]', normalized):
        normalized = "f_" + normalized
    return normalized

def infer_field_type(value):
    """
    Simple approach to map Python types to Azure Search field types.
    Adjust logic based on your requirements (e.g., for arrays, etc.).
    """
    if isinstance(value, bool):
        return SearchFieldDataType.Boolean
    elif isinstance(value, int):
        return SearchFieldDataType.Int64
    elif isinstance(value, float):
        return SearchFieldDataType.Double
    else:
        # For strings, lists, or anything else, store as string.
        # If you have lists, consider using Collection(Edm.String).
        return SearchFieldDataType.String


def build_dynamic_fields_from_json(flattened_json):
    """
    Given flattened JSON (key->value), build a list of Field objects
    to be used in the index definition.
    """
    fields = []
    # We will create a "dynamic" definition for each field we find.
    # We'll treat strings as `SearchableField` and numeric/bool as `SimpleField`.
    for k, v in flattened_json.items():
        normalized_key = normalize_field_name(k)
        field_type = infer_field_type(v)
        # If it's a string type, we can make it a 'SearchableField'
        if field_type == SearchFieldDataType.String:
            fields.append(SearchableField(name=normalized_key, type=field_type))
        else:
            # numeric or boolean
            fields.append(SimpleField(name=normalized_key, type=field_type, filterable=True, sortable=True))
    return fields


# ------------------------------------------------------------------------------
# 3) Create or Update the Index Dynamically
# ------------------------------------------------------------------------------
def create_or_update_index(index_name: str, sample_document: dict):
    """
    Create or update the index definition, pulling field names/types from a sample doc.
    Preserves existing documents when possible.
    """
    try:
        existing = get_search_index_client().get_index(index_name)
        # Check if we need to handle vector dimension mismatch
        existing_vector_dim = None
        for f in existing.fields:
            if getattr(f, "name", None) == "contentVector":
                existing_vector_dim = getattr(f, "vector_search_dimensions", None)
                break
        
        # Only delete index if vector dimensions actually changed
        if existing_vector_dim is not None and existing_vector_dim != azure_oai.EMBEDDING_DIM:
            print(f"Deleting index '{index_name}' due to vector dimension mismatch...")
            get_search_index_client().delete_index(index_name)
            # Wait for deletion to complete
            import time
            time.sleep(2)
            # Create new index with correct dimensions
            index = _build_index_definition(index_name, sample_document)
            result = get_search_index_client().create_index(index)
            return f"Index '{result.name}' recreated with new vector dimensions.", True
        else:
            # Try to update existing index without losing data
            print(f"Updating existing index '{index_name}' schema...")
            try:
                # Build new index definition
                new_index = _build_index_definition(index_name, sample_document)
                
                # Update the existing index (this preserves documents)
                result = get_search_index_client().create_or_update_index(new_index)
                return f"Index '{result.name}' schema updated successfully.", True
            except Exception as update_error:
                # If update fails due to schema conflicts, try to handle gracefully
                error_str = str(update_error)
                if "CannotChangeExistingField" in error_str or "OperationNotAllowed" in error_str:
                    print(f"Schema conflict detected. Attempting to add new fields only...")
                    try:
                        # Try to add only new fields to existing index
                        _add_new_fields_to_existing_index(index_name, sample_document)
                        return f"Index '{index_name}' updated with new fields only.", True
                    except Exception as add_error:
                        print(f"Failed to add new fields: {add_error}")
                        # Last resort: delete and recreate
                        print(f"Recreating index '{index_name}' due to schema conflicts...")
                        get_search_index_client().delete_index(index_name)
                        time.sleep(2)
                        index = _build_index_definition(index_name, sample_document)
                        result = get_search_index_client().create_index(index)
                        return f"Index '{result.name}' recreated after schema conflict resolution.", True
                else:
                    raise update_error
                    
    except Exception as e:
        # If get_index fails, we'll proceed to create
        print(f"Creating new index '{index_name}'...")
        index = _build_index_definition(index_name, sample_document)
        result = get_search_index_client().create_index(index)
        return f"Index '{result.name}' created successfully.", True

def _build_index_definition(index_name: str, sample_document: dict) -> SearchIndex:
    """Build the SearchIndex definition from sample document."""
    # Flatten the sample document (in case there's one-level nesting).
    flattened_sample = flatten_json(sample_document)

    # Build dynamic fields
    dynamic_fields = build_dynamic_fields_from_json(flattened_sample)

    # Always define a "key" field. We'll name it "id" here.
    key_field = SimpleField(name="id", type="Edm.String", key=True)

    # Define the embedding vector field
    vector_field = SearchField(
        name="contentVector",
        type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
        searchable=True,
        vector_search_dimensions=azure_oai.EMBEDDING_DIM,
        vector_search_profile_name="myHnswProfile"
    )

    # Also define a "content" field where we store full concatenated text
    # for semantic search and/or normal text queries
    content_field = SearchableField(name="content", type="Edm.String")

    # Final list of fields
    fields = [key_field] + dynamic_fields + [content_field, vector_field]

    # Vector search config
    vector_search = VectorSearch(
        algorithms=[
            HnswAlgorithmConfiguration(name="myHnsw")
        ],
        profiles=[
            VectorSearchProfile(
                name="myHnswProfile",
                algorithm_configuration_name="myHnsw",
            )
        ],
    )

    # Optional: semantic config
    semantic_config = SemanticConfiguration(
        name="my-semantic-config",
        prioritized_fields=SemanticPrioritizedFields(
            title_field=SemanticField(field_name="id"),
            content_fields=[SemanticField(field_name="content")]
        )
    )
    semantic_search = SemanticSearch(configurations=[semantic_config])

    # Create the search index
    index = SearchIndex(
        name=index_name,
        fields=fields,
        vector_search=vector_search,
        semantic_search=semantic_search
    )
    
    return index

def _add_new_fields_to_existing_index(index_name: str, sample_document: dict):
    """Add new fields to existing index without losing data."""
    try:
        # Get existing index
        existing_index = get_search_index_client().get_index(index_name)
        
        # Get new fields from sample document
        flattened_sample = flatten_json(sample_document)
        new_fields = build_dynamic_fields_from_json(flattened_sample)
        
        # Find existing field names
        existing_field_names = {f.name for f in existing_index.fields}
        
        # Filter out fields that already exist
        fields_to_add = [f for f in new_fields if f.name not in existing_field_names]
        
        if not fields_to_add:
            print(f"No new fields to add to index '{index_name}'")
            return
        
        # Add new fields to existing index
        for field in fields_to_add:
            try:
                # This is a simplified approach - in practice, you might need to use
                # the Azure Search REST API to add fields to an existing index
                print(f"Note: Field '{field.name}' would need to be added via REST API")
            except Exception as e:
                print(f"Warning: Could not add field '{field.name}': {e}")
                
    except Exception as e:
        print(f"Error adding new fields to existing index: {e}")
        raise

# ------------------------------------------------------------------------------
# 4) Load JSON Docs and Upsert
# ------------------------------------------------------------------------------
def load_json_into_azure_search(index_name, json_docs):
    """
    Takes a list of JSON documents. For each:
      1) Flatten the JSON.
      2) Build an embedding manually via get_embedding().
      3) Upsert to Azure Search with 'contentVector'.
    """
    if not json_docs:
        return "No documents to process.", False

    # 6a) Create/Update the index with the first doc as a template
    sample_doc = json_docs[0]
    message, result = create_or_update_index(index_name, sample_doc)
    if not result:
        return message, False

    # 6b) Create a SearchClient
    search_client = get_search_client(index_name)

    # 6c) Convert each doc to final structure for upserting
    actions = []
    for i, doc in enumerate(json_docs):
        flattened = flatten_json(doc)
        doc_id = f"doc-{i}"

        # We'll build a 'content' string from all string fields
        text_parts = []
        for k, v in flattened.items():
            if isinstance(v, str):
                text_parts.append(v)
        combined_text = " ".join(text_parts) if text_parts else ""

        # Manually get embeddings
        embedding_vector = azure_oai.get_embedding(combined_text)

        # Prepare final doc
        final_doc = {
            "id": doc_id,
            "content": combined_text,
            "contentVector": embedding_vector
        }
        # Add flattened fields using normalized keys
        for k, v in flattened.items():
            normalized_key = normalize_field_name(k)
            # If the value is a list, join it into a string
            if isinstance(v, list):
                final_doc[normalized_key] = " ".join(map(str, v))
            else:
                final_doc[normalized_key] = v

        actions.append(final_doc)

    # 6d) Upsert in bulk
    try:
        results = search_client.upload_documents(documents=actions)
        print(f"Upserted {len(actions)} documents into index '{index_name}'.")
        return "All document indexed", True
    except Exception as e:
        return f"Failed to index documents: {e}", False

def search_query(index_name, query):
    """
    Search Azure Search index with a query string.
    """
    search_client = get_search_client(index_name)
    try:
        query_vector = azure_oai.get_embedding(query)

        # Execute a vector search with semantic ranking enabled.
        results = search_client.search(
            search_text="",
            vector_queries=[{"vector": query_vector, "fields": "contentVector", "k": 5,  "kind": "vector"}],
            query_type="semantic"
        )
        return list(results)
    except Exception as e:
        print(f"Search failed: {e}")
        return []
    
def index_exists(index_name):
    """
    Check if an index exists in Azure Search.
    """
    search_index_client = get_search_index_client()
    try:
        index = search_index_client.get_index(index_name)
        return index is not None
    except Exception as e:
        return False

def get_index_document_count(index_name: str) -> int:
    """
    Get the current number of documents in an Azure Search index.
    """
    try:
        search_client = get_search_client(index_name)
        # Use a simple search with no filters to count all documents
        results = search_client.search(search_text="", top=0, include_total_count=True)
        return results.get_count() or 0
    except Exception as e:
        print(f"Error getting document count for index '{index_name}': {e}")
        return -1

def list_index_documents(index_name: str, top: int = 10) -> list:
    """
    List the first few documents in an Azure Search index for debugging.
    """
    try:
        search_client = get_search_client(index_name)
        results = search_client.search(search_text="", top=top)
        return list(results)
    except Exception as e:
        print(f"Error listing documents from index '{index_name}': {e}")
        return []