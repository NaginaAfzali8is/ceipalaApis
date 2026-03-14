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
        # 1. CLEANING: Remove parentheses and special characters
        clean_query = job_req.replace('(', '').replace(')', '').strip()
        
        # 2. KEYWORD LOGIC: Split words to ensure we don't return 0 results
        # This handles "Test Data Engineer (TDM)" by looking for each word
        keywords = clean_query.lower().replace("senior", "").replace("junior", "").split()
        
        filter_parts = []
        for word in keywords:
            if len(word) > 2:
                filter_parts.append(f"data->>job_title.ilike.%{word}%")
                filter_parts.append(f"data->>resume_text.ilike.%{word}%")
        
        filter_string = ",".join(filter_parts)

        # 3. GREEDY FETCH: Use select("*") to get all data like your old code
        response = supabase.table("parsed_resumes") \
            .select("*") \
            .or_(filter_string) \
            .limit(50) \
            .execute()

        all_matches = response.data

        # 4. SMART FILTER: Apply your CEO's Senior/Lead requirement
        is_senior_req = "senior" in job_req.lower() or "lead" in job_req.lower()
        final_list = []
        
        for cand in all_matches:
            # Check the whole data object for senior keywords if required
            text_to_check = str(cand.get('data', '')).lower()
            
            if is_senior_req:
                senior_keywords = ["senior", "lead", "sr.", "principal", "years exp"]
                if any(word in text_to_check for word in senior_keywords):
                    final_list.append(cand)
            else:
                # If not a senior req, include everyone found in the greedy search
                final_list.append(cand)

        return {
            "total": len(final_list), 
            "candidates": final_list
        }

    except Exception as e:
        return {"error": str(e), "total": 0, "candidates": []}


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
