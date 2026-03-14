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
        # 1. Clean and Lowercase
        clean_query = job_req.replace('(', '').replace(')', '').lower().strip()
        words = [w for w in clean_query.split() if len(w) > 2 and w not in ["senior", "junior", "lead"]]
        
        # 2. Generate Pairs (Bigrams)
        # If words are [Test, Data, Engineer], pairs are ["Test Data", "Data Engineer"]
        pairs = [" ".join(words[i:i+2]) for i in range(len(words)-1)]
        
        # If it's only one word, just use that word
        search_terms = pairs if pairs else words
        
        # 3. Build the OR filter for Supabase
        filter_parts = []
        for term in search_terms:
            filter_parts.append(f"data->>job_title.ilike.%{term}%")
            filter_parts.append(f"data->>resume_text.ilike.%{term}%")
        
        filter_string = ",".join(filter_parts)

        # 4. Fetch from Supabase
        response = supabase.table("parsed_resumes").select("*").or_(filter_string).limit(50).execute()
        all_matches = response.data

        # 5. Senior/Lead Filter (Keep your original logic)
        is_senior_req = "senior" in job_req.lower() or "lead" in job_req.lower()
        final_list = []
        for cand in all_matches:
            text_to_check = str(cand.get('data', '')).lower()
            if is_senior_req:
                if any(word in text_to_check for word in ["senior", "lead", "sr.", "principal"]):
                    final_list.append(cand)
            else:
                final_list.append(cand)

        return {"total": len(final_list), "candidates": final_list}

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
