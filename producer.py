import time
import mysql.connector
from kafka import KafkaProducer
import json
from datetime import date, datetime
from decimal import Decimal

# MySQL 연결 설정
db_config = {
    'user': 'root',
    'password': '12340131',
    'host': 'hellovision.c3gk86ic62pt.ap-northeast-2.rds.amazonaws.com',
    'port': 3306,
    'database': 'hellovision'
}

# Kafka 설정
bootstrap_servers = ['localhost:9092']
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v, default=json_serializer).encode('utf-8'),
    acks='all',
    max_in_flight_requests_per_connection=5
)

def json_serializer(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError("Type %s not serializable" % type(obj))

def fetch_data(query):
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    connection.close()
    return rows

def send_event(table_name, event_type, record):
    event = {
        'table_name': table_name,
    }
    
    # 자동으로 record_id 설정
    table_key_map = {
        'USERS': 'USER_ID',
        'SPOTIFY': 'SPOTIFY_ID',
        'LIKES': 'LIKE_ID',
        'REVIEW': 'REVIEW_ID',
        'VOD': 'VOD_ID',
        'SERIES': 'SERIES_ID',
        'SEASON': 'SEASON_ID',
        'EPISODE': 'EPISODE_ID',
        'MOVIES': 'MOVIE_ID',
        'KIDS': 'K_SERIES_ID',
        'KIDS_SEASON': 'K_SEASON_ID',
        'KIDS_EPISODE': 'K_EPISODE_ID',
        'ACTOR': 'ACTOR_TABLE_ID',
        'MOVIE_ACTOR': 'MOVIE_ID'
    }
    
    if event_type == 'insert':
        event['new_data'] = record
        event['record_id'] = record[table_key_map.get(table_name)]
        topic_name = 'insert'
    elif event_type == 'delete':
        event['record_id'] = record['record_id']
        topic_name = 'deleted_records'
    elif event_type == 'update':
        event['record_id'] = record['record_id']
        event['updated_data'] = json.loads(record['updated_data'])
        topic_name = 'updated_records'
    else:
        raise ValueError(f"Invalid event_type: {event_type}")

    producer.send(topic_name, value=event)

def remove_log_entry(table, record_id):
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    cursor.execute(f"DELETE FROM {table} WHERE id = %s", (record_id,))
    connection.commit()
    cursor.close()
    connection.close()

# 초기 데이터 전송
def send_initial_data():
    tables = {
        'USERS': fetch_data("SELECT * FROM USERS"),
        'SPOTIFY': fetch_data("SELECT * FROM SPOTIFY"),
        'LIKES': fetch_data("SELECT * FROM LIKES"),
        'REVIEW': fetch_data("SELECT * FROM REVIEW"),
        'VOD': fetch_data("SELECT * FROM VOD"),
        'SERIES': fetch_data("SELECT * FROM SERIES"),
        'MOVIES': fetch_data("SELECT * FROM MOVIES"),
        'KIDS': fetch_data("SELECT * FROM KIDS"),
        'SEASON': fetch_data("SELECT * FROM SEASON"),
        'ACTOR': fetch_data("SELECT * FROM ACTOR"),
        'EPISODE': fetch_data("SELECT * FROM EPISODE"),
        'KIDS_EPISODE': fetch_data("SELECT * FROM KIDS_EPISODE"),
        'KIDS_SEASON': fetch_data("SELECT * FROM KIDS_SEASON"),
        'MOVIE_ACTOR': fetch_data("SELECT * FROM MOVIE_ACTOR")
    }

    for table_name, records in tables.items():
        for record in records:
            send_event(table_name, 'insert', record)

send_initial_data()

# 주기적인 업데이트 및 삭제 확인
while True:
    try:
        check_inserted_records()
        check_deleted_records()
        check_updated_records()
    except Exception as e:
        print(f"Error: {e}")
        # 오류 처리: 예외 발생 시 메시지를 출력하고, 다음 반복을 진행합니다.
    
    time.sleep(10)  # 10초마다 확인

# 마지막에 producer flush 및 close 호출
producer.flush()
producer.close()
