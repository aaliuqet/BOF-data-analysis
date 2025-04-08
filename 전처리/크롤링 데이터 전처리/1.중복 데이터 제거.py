from pymongo import MongoClient

# MongoDB 클라이언트 및 컬렉션 설정
client = MongoClient(host="", port=, username='', password='')
db = client['BOF2024_crawling']
collection = db['tiktok_crawling'] 

# 중복된 제목과 URL을 가진 문서 중 하나만 남기고 나머지 삭제
deleted_count = 0
pipeline = [
    {"$group": {"_id": {"title": "$제목", "url": "$URL"}, "count": {"$sum": 1}, "ids": {"$push": "$_id"}}},
    {"$match": {"count": {"$gt": 1}}}
]
duplicates = list(collection.aggregate(pipeline))

for duplicate in duplicates:
    for _id in duplicate['ids'][1:]:
        result = collection.delete_one({"_id": _id})
        if result.deleted_count == 1:
            deleted_count += 1

print(f"{deleted_count}개의 중복된 문서가 삭제되었습니다.")
