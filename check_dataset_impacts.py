import asyncio
from app.supabase import require_supabase

async def check_dataset_impacts():
    """Query and display the dataset_impacts table rows."""
    supabase = require_supabase()
    
    print("Querying dataset_impacts table...")
    result = supabase.table("dataset_impacts").select("*").execute()
    
    if not result.data:
        print("No records found in dataset_impacts table.")
        return

    headers = list(result.data[0].keys())
    header_row = " | ".join(f"{h[:15]:<15}" for h in headers)
    print("\n" + "=" * len(header_row))
    print(header_row)
    print("=" * len(header_row))
    
    for row in result.data:
        values = [str(row.get(h, ""))[:15] for h in headers]
        print(" | ".join(f"{v:<15}" for v in values))
    
    print("=" * len(header_row))
    print(f"Total records: {len(result.data)}")

if __name__ == "__main__":
    asyncio.run(check_dataset_impacts()) 