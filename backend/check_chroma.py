from database import get_chroma_collection

collection = get_chroma_collection("scholarship_rules")

data = collection.get()

print("총 문서 수:", len(data["ids"]))
print("키 목록:", data.keys())

print("\n=== 첫 3개 metadata ===")
for meta in data["metadatas"][:3]:
    print(meta)

print("\n=== 첫 3개 document ===")
for doc in data["documents"][:3]:
    print(doc)
    print("-" * 50)