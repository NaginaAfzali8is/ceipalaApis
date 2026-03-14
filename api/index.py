import os
from fastapi import FastAPI, HTTPException, Query
from typing import List
from supabase import create_client, Client
from dotenv import load_dotenv

# This line MUST be here to read your .env file
load_dotenv()

app = FastAPI()

# Get variables
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_ANON_KEY")

# Safety check: If these are missing, the app will tell you why
if not url or not key:
    raise ValueError("SUPABASE_URL or SUPABASE_ANON_KEY is missing from .env file")

supabase: Client = create_client(url, key)
@app.get("/api/match-candidates")
async def match_candidates(job_req: str):
    try:
        # 1. Clean the input (remove parentheses and extra spaces)
        clean_query = job_req.replace('(', '').replace(')', '').strip()
        
        # 2. Split into keywords to match your SQL logic
        keywords = clean_query.split()
        
        # 3. Build a filter string for Supabase
        # This mimics your 'EXISTS (unnest(string_to_array...))' logic
        filter_parts = []
        for word in keywords:
            if len(word) > 2: # Ignore small words like 'at', 'in'
                filter_parts.append(f"data->>job_title.ilike.%{word}%")
                filter_parts.append(f"data->>resume_text.ilike.%{word}%")
        
        filter_string = ",".join(filter_parts)

        # 4. Execute the query using .or_() for keyword matching
        response = supabase.table("parsed_resumes") \
            .select("id, data->full_name, data->job_title") \
            .or_(filter_string) \
            .limit(10) \
            .execute()

        return {
            "total": len(response.data),
            "candidates": response.data
        }
    except Exception as e:
        return {"error": str(e), "candidates": []}


# NEW: Fetch Candidates by Array of IDs
@app.get("/api/fetch-by-ids")
async def fetch_by_ids(ids: str = Query(...)):
    # Expects ids as a comma-separated string: "1,2,3"
    try:
        id_list = ids.split(",")
        response = supabase.table("ceipal_applicant_details").select("*").in_("id", id_list).execute()
        return {"candidates": response.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    
@app.get("/api/health")
async def health_check():
    return {"status": "connected", "database": "supabase_rest_api"}
