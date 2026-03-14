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
        # Step 1: Greedy Search (Ensures zero skips)
        # FIX: We remove parentheses here because they break the .ilike query in the API
        clean_req = job_req.replace("(", "").replace(")", "")
        
        core_role = clean_req.lower().replace("senior", "").replace("junior", "").strip()

        # We query using the cleaned role
        response = supabase.table("parsed_resumes").select("*").or_(
            f"data->>job_title.ilike.%{core_role}%,data->>resume_text.ilike.%{core_role}%"
        ).limit(50).execute()

        all_matches = response.data

        # Step 2: Smart Filter for "Senior" (CEO Requirement)
        is_senior_req = "senior" in job_req.lower() or "lead" in job_req.lower()

        final_list = []
        for cand in all_matches:
            # Safely get the data dictionary
            candidate_data = cand.get('data', {})
            text_to_check = str(candidate_data).lower()
            
            # If we need a senior, we look for senior keywords
            if is_senior_req:
                senior_keywords = ["senior", "lead", "sr.", "principal", "years exp"]
                if any(word in text_to_check for word in senior_keywords):
                    final_list.append(cand)
            else:
                final_list.append(cand)

        return {"total": len(final_list), "candidates": final_list}

    except Exception as e:
        # Using print here helps you see the error in Vercel Logs
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

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
