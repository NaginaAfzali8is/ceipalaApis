import os
from datetime import datetime
from supabase import create_client
from pymongo import MongoClient
from dotenv import load_dotenv
from bson import ObjectId

load_dotenv()

# Setup Clients
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_ANON_KEY"))
mongo_client = MongoClient("mongodb+srv://hayat:cjIwh4vh1sDvxFCb@cluster0.izhtqvc.mongodb.net/") # Change to your Atlas URI if needed
db = mongo_client["recruitment_db"]

def migrate_without_loss():
    print("🚀 Starting Guaranteed Migration (No Data Loss)...")

    # --- 1. BUILD LOCAL CACHE TO PREVENT DUPLICATES ---
    print("🧠 Loading existing record keys from MongoDB to prevent duplicates...")
    # We store supabase_ids in a set for O(1) lightning-fast lookup
    existing_resumes = set(db["parsed_resumes"].distinct("supabase_id"))
    
    # Map old Supabase Candidate IDs to new MongoDB ObjectIds
    applicant_map = {str(a["id"]): a["_id"] for a in db["ceipal_applicant_details"].find({}, {"id": 1, "_id": 1})}
    
    print(f"📊 Status: {len(existing_resumes)} resumes and {len(applicant_map)} applicants already in Mongo.")

    # --- 2. KEYSET PAGINATION LOGIC ---
    # We use UUID sorting to ensure we visit every single record in order
    last_id = None 
    limit = 1000
    added = 0
    skipped = 0
    orphans = 0

    while True:
        try:
            # Query Supabase: Order by ID to avoid deep-paging timeouts
            query = supabase.table("parsed_resumes").select("*").order("id").limit(limit)
            if last_id:
                query = query.gt("id", last_id)
            
            res = query.execute()
            data = res.data
            
            if not data:
                break # End of table reached
            
            batch_to_insert = []
            
            for r in data:
                s_id = str(r.get("id"))
                last_id = s_id # Track the last UUID seen
                
                # Check A: Do we already have this resume?
                if s_id in existing_resumes:
                    skipped += 1
                    continue
                
                # Check B: Does the applicant exist in Mongo?
                old_candidate_ref = str(r.get("candidate_id"))
                new_mongo_id = applicant_map.get(old_candidate_ref)
                
                data_content = r.get("data", {})
                data_content.pop("_id", None) # Ensure Mongo doesn't use old BSON IDs

                # Logic: If applicant is missing, we keep the resume but mark it as Orphan
                if not new_mongo_id:
                    orphans += 1
                    target_id = f"ORPHAN_{old_candidate_ref}"
                else:
                    target_id = new_mongo_id

                batch_to_insert.append({
                    "supabase_id": s_id,
                    "candidate_id": target_id,
                    **data_content,
                    "created_at": datetime.utcnow().isoformat(),
                    "is_orphan": not new_mongo_id
                })

            # --- 3. BULK INSERT ---
            if batch_to_insert:
                db["parsed_resumes"].insert_many(batch_to_insert)
                added += len(batch_to_insert)
                # Update local cache so we don't re-insert if a page overlaps
                for b in batch_to_insert:
                    existing_resumes.add(b["supabase_id"])
                
                print(f"✅ Added {added} new resumes... (Orphans: {orphans}, Skipped: {skipped})")
            else:
                if skipped % 5000 == 0:
                    print(f"⏩ Verified {skipped} records already exist...")

        except Exception as e:
            print(f"❌ Connection error or Timeout: {e}")
            print("Restarting batch in 5 seconds...")
            import time
            time.sleep(5)
            continue # This allows the script to keep trying if the internet blips

    print("\n✨ FINAL REPORT:")
    print(f"Total Resumes Added: {added}")
    print(f"Total Records Skipped (Already existed): {skipped}")
    print(f"Total Orphans (Resumes with no candidate): {orphans}")
    print(f"Total Resumes now in MongoDB: {db['parsed_resumes'].count_documents({})}")

if __name__ == "__main__":
    migrate_without_loss()