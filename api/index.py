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
        clean_query = job_req.replace('(', '').replace(')', '').lower().strip()
        words = [w for w in clean_query.split() if len(w) > 2 and w not in ["senior", "junior", "lead"]]
        pairs = [" ".join(words[i:i+2]) for i in range(len(words)-1)]
        search_terms = pairs if pairs else words
        
        filter_parts = [f"data->>job_title.ilike.%{t}%" for t in search_terms] + \
                       [f"data->>resume_text.ilike.%{t}%" for t in search_terms]
        
        response = supabase.table("parsed_resumes").select("*").or_(",".join(filter_parts)).limit(50).execute()
        all_matches = response.data

        # NEW: High-Precision TDM Keywords
        # If the job is "Test Data", we look for these specific industry terms
        tdm_identifiers = ["masking", "synthetic", "subsetting", "tdm", "provisioning", "etl", "qa automation"]
        
        final_list = []
        for cand in all_matches:
            text = str(cand.get('data', '')).lower()
            
            # Smart Logic: If it's a TDM job, check for TDM tools/tasks
            is_tdm_job = "test data" in clean_query
            has_tdm_context = any(word in text for word in tdm_identifiers)
            
            if is_tdm_job and not has_tdm_context:
                continue # Skip Data Analysts like Vraj who don't do TDM tasks
                
            # Keep your original Senior Filter
            is_senior_req = "senior" in job_req.lower() or "lead" in job_req.lower()
            if is_senior_req:
                if any(w in text for w in ["senior", "lead", "sr.", "principal"]):
                    final_list.append(cand)
            else:
                final_list.append(cand)

        return {"total": len(final_list), "candidates": final_list}



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
