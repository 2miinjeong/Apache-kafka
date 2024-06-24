from kafka import KafkaConsumer
from pymongo import MongoClient
from bson import ObjectId
import json

# MongoDB 연결 설정
mongo_client = MongoClient('mongodb://wang:0131@3.37.201.211:27017')
mongo_db = mongo_client['hellody']

# Kafka Consumer 설정
consumer = KafkaConsumer(
    'insert',
    'deleted_records',
    'updated_records',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',  # 가장 오래된 오프셋부터 읽기 시작 (첫 실행 시)
    enable_auto_commit=True,  # 자동으로 오프셋 커밋
    group_id='my-group'  # Consumer 그룹 ID 설정
)

# 각 테이블에 대한 고유한 ID 필드 설정
id_field_map = {
    'USERS': 'USER_ID',
    'SPOTIFY': 'SPOTIFY_ID',
    'REVIEW': 'REVIEW_ID',
    'LIKES': 'LIKE_ID',
    'VOD': 'VOD_ID',
    'SERIES': 'SERIES_ID',
    'SEASON': 'SEASON_ID',
    'EPISODE': 'EPISODE_ID',
    'MOVIES': 'MOVIE_ID',
    'MOVIE_ACTOR': 'MOVIE_ID',
    'ACTOR': 'ACTOR_TABLE_ID',
    'KIDS': 'K_SERIES_ID',
    'KIDS_SEASON': 'K_SEASON_ID',
    'KIDS_EPISODE': 'K_EPISODE_ID'
}

# 메시지 처리
for message in consumer:
    try:
        table_name = message.value['table_name']
        record_id = message.value['record_id']
        topic = message.topic

        # MongoDB 컬렉션 선택
        mongo_collection = mongo_db[table_name]

        # 고유한 ID 필드 가져오기
        unique_field = id_field_map.get(table_name)

        if not unique_field:
            print(f"Unknown table name: {table_name}")
            continue

        if topic == 'insert':
            new_data = message.value['new_data']
            # 고유한 _id 설정 (새 ObjectId 생성)
            new_data['_id'] = ObjectId()
            # unique_field를 기준으로 중복 확인
            if not mongo_collection.find_one({unique_field: record_id}):
                result = mongo_collection.insert_one(new_data)
                print(f"MongoDB Insert Result: {result.inserted_id}")
            else:
                print(f"Duplicate entry found for {unique_field}: {record_id}")
            
            if table_name == 'USERS':
                recommend_collection = mongo_db['recommend_list']
                # recommend_list에 중복 확인 후 삽입
                if not recommend_collection.find_one({'user_id': record_id}):
                    recommend_collection.insert_one({'user_id': record_id})
                    print(f"Recommend list entry created for user_id: {record_id}")
                else:
                    print(f"Duplicate entry found in recommend_list for user_id: {record_id}")

        elif topic == 'deleted_records':
            result = mongo_collection.delete_one({unique_field: record_id})
            print(f"MongoDB Delete Result: {result.deleted_count}")

        elif topic == 'updated_records':
            updated_data = message.value['updated_data']
            # JSON 문자열을 딕셔너리로 변환
            if isinstance(updated_data, str):
                updated_data = json.loads(updated_data)

            print(f"Received from Kafka: {message.value}")  # 로그 추가

            result = mongo_collection.update_one({unique_field: record_id}, {'$set': updated_data})
            print(f"MongoDB Update Result: {result.modified_count}")  # 로그 추가

    except Exception as e:
        print(f"Error processing message: {e}")
