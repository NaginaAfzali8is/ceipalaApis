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

from datetime import datetime, timedelta

from datetime import datetime, timedelta

@app.get("/api/match-candidates")
async def match_candidates(job_req: str):
    try:
        # 1. Date Logic
        three_months_ago = (datetime.now() - timedelta(days=90)).isoformat()

        clean_query = job_req.replace('(', '').replace(')', '').lower().strip()
        words = [w for w in clean_query.split() if len(w) > 2 and w not in ["senior", "junior", "lead"]]
        search_terms = [" ".join(words[i:i+2]) for i in range(len(words)-1)] if len(words) > 1 else words
        
        filter_parts = [f"data->>job_title.ilike.%{t}%" for t in search_terms] + \
                       [f"data->>resume_text.ilike.%{t}%" for t in search_terms]

        # 2. THE JOIN QUERY
        # We select from parsed_resumes and "inner join" ceipal_applicant_details
        # We use !inner to force the date filter to apply
        response = supabase.table("parsed_resumes").select(
            "id, data, candidate_id, ceipal_applicant_details!inner(created_at)"
        ).or_(
            ",".join(filter_parts)
        ).gte(
            "ceipal_applicant_details.created_at", three_months_ago
        ).limit(50).execute()
        
        all_matches = response.data
        tdm_keywords = ["masking", "synthetic", "subsetting", "tdm", "provisioning", "etl", "qa automation"]
        
        final_list = []
        for cand in all_matches:
            data = cand.get('data', {})
            text = str(data).lower()
            
            if "test data" in clean_query and not any(k in text for k in tdm_keywords):
                continue
            
            final_list.append({
                "id": cand.get("id"),
                "job_title": data.get("job_title"),
                "candidate_id": cand.get("candidate_id"),
                        "resume_url": data.get("resume_url"),  # ✅ added here
                "resume_summary": data.get("resume_text", "")[:3000] # Increased to 3k for safety
            })

      # SLICE THE LIST HERE: Change 15 to 10 if you want even fewer
        limited_list = final_list[:15]

        return {"total": len(limited_list), "candidates": limited_list}
       # // return {"total": len(final_list), "candidates": final_list}

    except Exception as e:
        # This will help you see the exact SQL error if the join fails
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
