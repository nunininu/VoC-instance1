import io
import json
import logging
import time
import os
from typing import Tuple

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from google.cloud import storage
from faster_whisper import WhisperModel

# ----- 설정 -----
GCS_KEY_PATH        = os.environ.get("GCS_KEY_PATH")
BUCKET_NAME         = "wh04-voc"
AUDIO_DIR           = "new/raw/ars/audio/"
CONSULTING_DIR      = "new/raw/ars/consulting/"
OUTPUT_DIR          = "new/raw/consulting/"

BOOTSTRAP_SERVER    = "kafka:9093"
ARS_TOPIC           = "voc-ars-raw"
CONSULTING_TOPIC    = "voc-consulting-raw"
TIMEOUT             = 10    # seconds
MAX_RETRIES         = 3
RETRY_DELAY         = 5     # seconds
RETRY_BACKOFF_MS    = 500   # ms

# ----- 로깅 -----
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ----- 공통 클라이언트 -----
model = WhisperModel("base", compute_type="int8", device="cpu")
consumer: KafkaConsumer
producer: KafkaProducer
storage_client: storage.Client
bucket: storage.Bucket


# ----- 유틸 함수 -----
def connect_kafka() -> None:
    """
    Kafka Producer & Consumer 생성 함수
    """
    global consumer, producer
    for attempt in range(10):
        try:
            consumer = KafkaConsumer(
                ARS_TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVER,
                enable_auto_commit=False,
                value_deserializer=lambda b: json.loads(b),
            )
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVER,
                acks="all",
                enable_idempotence=True,
                retries=MAX_RETRIES,
                retry_backoff_ms=RETRY_BACKOFF_MS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            logger.info("Kafka 연결 성공!")
            break
        except:
            logger.warning(f"[{attempt+1}/10] Kafka 연결 실패, 2초 후 재시도..")
            time.sleep(2)
    else:
        logger.error("Kafka 연결 실패. 종료되지 않지만 연결은 실패 상태.")
        consumer = None
        producer = None

def connect_gcs_client() -> None:
    """
    GCP 및 GCS 연동 함수
    """
    global storage_client, bucket
    storage_client = storage.Client.from_service_account_json(GCS_KEY_PATH)
    bucket = storage_client.bucket(BUCKET_NAME)

def download_blob_to_bytes(path: str) -> bytes:
    """
    데이터를 다운로드 하는 함수

    :param path: 다운로드 받을 파일의 경로

    :return: 다운로드 완료 된 데이터 (bytes 타입)
    """
    blob = bucket.blob(path)
    if not blob.exists():
        raise FileNotFoundError(f"GCS object not found: {path}")
    return blob.download_as_bytes()

def process_stt(audio_bytes: bytes) -> Tuple[str, int]:
    """
    STT 처리 함수

    :param audio_bytes: 텍스트(str)로 변환할 음성 데이터

    :return: STT 완료된 텍스트 데이터와 음성 데이터의 시간(초)
    """
    audio_stream = io.BytesIO(audio_bytes)
    segments, info = model.transcribe(audio_stream, language="ko")
    text     = "".join(seg.text for seg in segments)
    duration = int(info.duration)
    return text, duration

def upload_json(path: str, data: dict) -> None:
    """
    json 파일 업로드 함수

    :param path: 업로드할 경로
    :param data: 업로드할 json 데이터
    """
    blob = bucket.blob(path)
    blob.upload_from_string(
        json.dumps(data, ensure_ascii=False),
        content_type="application/json"
    )
    logger.info(f"Uploaded to gs://{BUCKET_NAME}/{path}")

def recieve_message(dt: str, cid: str) -> None:
    """
    Kafka Consumer에서 받은 데이터를 처리하는 함수

    :param dt: 문의 날짜
    :param cid: 문의 고유 ID
    """
    json_path  = f"{CONSULTING_DIR}{dt}_consulting_{cid}.json"
    audio_path = f"{AUDIO_DIR}{dt}_audio_{cid}.mp3"
    save_path   = f"{OUTPUT_DIR}{dt}_consulting_{cid}.json"

    try:
        # JSON 다운로드
        json_bytes = download_blob_to_bytes(json_path)
        json_data  = json.loads(json_bytes)

        # Audio 다운로드 & STT
        audio_bytes = download_blob_to_bytes(audio_path)
        text, duration = process_stt(audio_bytes)

        # 데이터 병합 및 업로드
        json_data.update({
            "consulting_content": text,
            "duration": duration
        })
        upload_json(save_path, json_data)

        logging.info("STT 완료 후 업로드 성공")
    except FileNotFoundError as e:
        logger.error(f"Not found: {e}")
    except Exception as e:
        logger.exception("STT processing failed")

def send_message(msg: dict) -> None:
    """
    텍스트 분석 서버로 메시지를 보내는 함수
    
    :param msg: 전달할 kafka 메시지
    """
    future = producer.send(CONSULTING_TOPIC, msg)
    logger.info("Kafka 메시지 전송중...")
    
    try:
        metadata = future.get(timeout=5)
        logger.info(f"메시지 전송 완료. {metadata.topic}[{metadata.partition}]@{metadata.offset}")
    except KafkaError as e:
        logger.error(f"메시지 전송 실패: {e}")
        
def main():
    """
    전체적인 로직을 처리하는 메인 함수
    """
    connect_kafka()
    connect_gcs_client()
    
    for msg in consumer:
        data = msg.value
        logger.info(f"메시지 수신: {data}")
        
        recieve_message(data["consulting_date"], data["consulting_id"], data["request_id"])
        send_message(data)
        
        consumer.commit()
        

if __name__ == "__main__":
    main()
