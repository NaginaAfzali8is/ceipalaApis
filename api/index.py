import os
from fastapi import FastAPI, HTTPException, Query
from typing import List
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime, timedelta        
from pydantic import BaseModel
from typing import List
import re
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId

# MongoDB Setup
MONGO_URI = os.getenv("MONGO_URI") # Update if using Atlas
client = AsyncIOMotorClient(MONGO_URI)
db = client["recruitment_db"]
resume_col = db["parsed_resumes"]

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
        ).limit(150).execute()
        
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



@app.post("/api/matchCandidatesLocationBased")
async def match_candidates_location_based(body: JobRequest):

    # 0. Extract raw values (Safety first)
    job_req = body.job_req or []
    target_city = body.target_city or ""
    target_state = body.target_state or ""
    target_country = body.target_country or ""

    try:
        # 1. Date Logic (90 Days - String comparison for ISO format)
        three_months_ago = (datetime.utcnow() - timedelta(days=90)).isoformat()

        # 2. Normalize keywords (Existing Flow)
        keywords = []
        for kw in job_req:
            clean = kw.replace('(', '').replace(')', '').replace(',', '').lower().strip()
            if clean:
                keywords.append(clean)

        # 3. Build search terms (Existing Flow: bi-grams + original)
        search_terms = []
        for kw in keywords:
            words = [w for w in kw.split() if len(w) > 2 and w not in ["senior", "junior", "lead"]]
            if len(words) > 1:
                search_terms.extend([" ".join(words[i:i+2]) for i in range(len(words)-1)])
            else:
                search_terms.append(kw)

        search_terms = list(set(search_terms))[:10]

        # --- 4. THE MONGODB FILTER (Skills AND Location AND Date) ---
        mongo_filter = {"$and": []}

        # A. Skills Filter (Regex OR)
        if search_terms:
            skill_pattern = "|".join([re.escape(t) for t in search_terms])
            mongo_filter["$and"].append({
                "$or": [
                    {"job_title": {"$regex": skill_pattern, "$options": "i"}},
                    {"resume_text": {"$regex": skill_pattern, "$options": "i"}}
                ]
            })

        # B. Location Filter (Must match any address field if provided)
        loc_inputs = [t.strip() for t in [target_city, target_state, target_country] 
                      if t and t.strip() and t.lower() != "not specified"]
        
        if loc_inputs:
            loc_pattern = "|".join([re.escape(t) for t in loc_inputs])
            mongo_filter["$and"].append({
                "$or": [
                    {"location": {"$regex": loc_pattern, "$options": "i"}},
                    {"city": {"$regex": loc_pattern, "$options": "i"}},
                    {"state": {"$regex": loc_pattern, "$options": "i"}},
                    {"country": {"$regex": loc_pattern, "$options": "i"}}
                ]
            })

        # C. Date Filter
        mongo_filter["$and"].append({"created_at": {"$gte": three_months_ago}})

        # 5. Execute Async Query
        cursor = resume_col.find(mongo_filter).sort("created_at", -1).limit(100)
        all_matches = await cursor.to_list(length=100)

        # 6. Domain/TDM Filter (Existing Flow)
        tdm_keywords = ["masking", "synthetic", "subsetting", "tdm", "provisioning", "etl", "qa automation"]
        final_list = []

        for cand in all_matches:
            # Safety: handle missing fields to prevent .lower() crash
            j_title = cand.get('job_title') or ""
            r_text = cand.get('resume_text') or ""
            combined_text = f"{j_title} {r_text}".lower()

            if any("test data" in kw for kw in keywords):
                if not any(k in combined_text for k in tdm_keywords):
                    continue

            # CRITICAL: Convert all ObjectIds to string to prevent 500 error
            final_list.append({
                "id": str(cand.get("_id")), 
                "job_title": j_title,
                "candidate_id": str(cand.get("candidate_id")),
                "resume_url": cand.get("resume_url"),
                "resume_summary": r_text[:3000]
            })

        return {
            "total": len(final_list),
            "candidates": final_list
        }

    except Exception as e:
        # This will send the actual error back to n8n so you can see why it failed
        import traceback
        print(traceback.format_exc()) # Check your terminal logs for this!
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
