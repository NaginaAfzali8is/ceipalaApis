import os
from fastapi import FastAPI, HTTPException, Query
from typing import List
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime, timedelta        
from pydantic import BaseModel
from typing import List

class JobRequest(BaseModel):
    job_req: List[str]
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

# Assuming your JobRequest looks like this:
class JobRequest(BaseModel):
    job_req: list
    target_city: str = None
    target_state: str = None
    target_country: str = None

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
        ).limit(200).execute()
        
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
       #// limited_list = final_list[:30]

       # // return {"total": len(limited_list), "candidates": limited_list}
        return {"total": len(final_list), "candidates": final_list}

    except Exception as e:
        # This will help you see the exact SQL error if the join fails
        return {"error": str(e), "total": 0, "candidates": []}


@app.post("/api/matchCandidates")
async def match_candidates(body: JobRequest):
    job_req = body.job_req 
    try:
        # 1. Date Logic
        three_months_ago = (datetime.now() - timedelta(days=90)).isoformat()

        # 2. Normalize keywords
        keywords = []
        for kw in job_req:
            # Adding .replace(',', '') here is the first line of defense
            clean = kw.replace('(', '').replace(')', '').replace(',', '').lower().strip()
            if clean:
                keywords.append(clean)

        # 3. Build search terms (bi-grams + original)
        search_terms = []
        for kw in keywords:
            words = [w for w in kw.split() if len(w) > 2 and w not in ["senior", "junior", "lead"]]

            if len(words) > 1:
                search_terms.extend([" ".join(words[i:i+2]) for i in range(len(words)-1)])
            else:
                search_terms.append(kw)

        # Remove duplicates
        search_terms = list(set(search_terms))

        # 4. Build OR filters
        filter_parts = []
        for term in search_terms:
            # Safety check: replace any remaining commas just in case
            clean_term = term.replace(',', '').strip()
            if clean_term:
                filter_parts.append(f"data->>job_title.ilike.%{clean_term}%")
                filter_parts.append(f"data->>resume_text.ilike.%{clean_term}%")

        # 5. Query Supabase
        response = supabase.table("parsed_resumes").select(
            "id, data, candidate_id, ceipal_applicant_details!inner(created_at)"
        ).or_(
            ",".join(filter_parts)
        ).gte(
            "ceipal_applicant_details.created_at", three_months_ago
        ).limit(100).execute()

        all_matches = response.data

        # 6. Optional domain filter (keep if needed)
        tdm_keywords = ["masking", "synthetic", "subsetting", "tdm", "provisioning", "etl", "qa automation"]

        final_list = []
        for cand in all_matches:
            data = cand.get('data', {})
            text = str(data).lower()

            # Optional filter logic
            if any("test data" in kw for kw in keywords):
                if not any(k in text for k in tdm_keywords):
                    continue

            final_list.append({
                "id": cand.get("id"),
                "job_title": data.get("job_title"),
                "candidate_id": cand.get("candidate_id"),
                "resume_url": data.get("resume_url"),
                "resume_summary": data.get("resume_text", "")[:3000]
            })

        return {
            "total": len(final_list),
            "candidates": final_list
        }

    except Exception as e:
        return {"error": str(e), "total": 0, "candidates": []}


import re
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient

# MongoDB Setup
MONGO_URI = os.getenv("MONGO_URI") # Update if using Atlas
client = AsyncIOMotorClient(MONGO_URI)
db = client["recruitment_db"]
resume_col = db["parsed_resumes"]

@app.post("/api/matchCandidatesLocationBased")
async def match_candidates_location_based(body: JobRequest):
    job_req = body.job_req 
    target_city = body.target_city
    target_state = body.target_state
    target_country = body.target_country

    try:
        three_months_ago = datetime.utcnow() - timedelta(days=90)

        # 1. Prepare search keywords
        keywords = []
        for kw in job_req:
            clean = re.sub(r'[(),]', '', kw).lower().strip()
            if clean:
                keywords.append(clean)

        # 2. Build the MongoDB Filter
        mongo_filter = {}

        # --- LOCATION FILTERS (Direct Fields) ---
        # We use regex for a 'contains' search similar to ILIKE
        if target_city:
            mongo_filter["location"] = {"$regex": target_city, "$options": "i"}
        
        # --- DATE FILTER ---
        mongo_filter["created_at"] = {"$gte": three_months_ago.isoformat()}

        # --- KEYWORD SEARCH (Direct Fields) ---
        if keywords:
            # Combining keywords into a regex pattern: "java|python|backend"
            regex_pattern = "|".join(keywords[:10])
            
            mongo_filter["$or"] = [
                {"job_title": {"$regex": regex_pattern, "$options": "i"}},
                {"resume_text": {"$regex": regex_pattern, "$options": "i"}},
                {"full_name": {"$regex": regex_pattern, "$options": "i"}}
            ]

        # 3. Execute Query
        cursor = resume_col.find(mongo_filter).sort("created_at", -1).limit(100)
        matches = await cursor.to_list(length=100)

        # 4. TDM Keyword Logic & Formatting
        tdm_keywords = ["masking", "synthetic", "subsetting", "tdm", "provisioning", "etl", "qa automation"]
        final_list = []

        for cand in matches:
            # Extract text for TDM check
            combined_text = f"{cand.get('job_title', '')} {cand.get('resume_text', '')}".lower()

            if any("test data" in kw for kw in keywords):
                if not any(k in combined_text for k in tdm_keywords):
                    continue

            final_list.append({
                "id": str(cand.get("_id")),
                "job_title": cand.get("job_title"),
                "candidate_id": str(cand.get("candidate_id")),
                "resume_url": cand.get("resume_url"),
                "location": cand.get("location"),
                "full_name": cand.get("full_name"),
                # Summary for preview
                "resume_summary": cand.get("resume_text", "")[:3000]
            })

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