import logging
import json
import time
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, File, Response, HTTPException
from kafka import KafkaProducer
from kafka.errors import KafkaError
from google.cloud import storage

# ----- 설정 -----
GCS_KEY_PATH        = "/home/joon/keys/vocabulary-459105-5603cb550881.json"
BUCKET_NAME         = "wh04-voc"
NON_ARS_JSON_DIR    = "tmp/raw/consulting/"
ARS_JSON_DIR        = "tmp/raw/ars/consulting/"
AUDIO_DIR           = "tmp/raw/ars/audio/"

TOPIC               = "voc-consulting-raw"
MAX_RETRIES         = 3
RETRY_BACKOFF_MS    = 500

# ----- 로깅 -----
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger("submit_service")

# ------ 공통 클라이언트 -----
producer: KafkaProducer
storage_client: storage.Client
bucket: storage.Bucket


# ----- Life cycle -----
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI Lifecycle 관리 함수  
    yield 이전 코드는 서버 시작 직후 실행되고,
    yield 이후 코드는 서버 종료 직후 실행된다.
    
    :param app: 실행 시킬 FastAPI 앱
    """
    global producer, storage_client, bucket
    logger.info("Kafka Producer 준비 완료…")
    
    for attempt in range(10):
        try:
            # Kafka Producer
            producer = KafkaProducer(
                bootstrap_servers="kafka:9093",
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
        producer = None

    # GCS Client
    storage_client = storage.Client.from_service_account_json(GCS_KEY_PATH)
    bucket = storage_client.bucket(BUCKET_NAME)
    yield
    logger.info("Kafka Producer를 종료합니다…")
    try:
        producer.flush(timeout=10)
    except Exception as e:
        logger.warning(f"요청 전송 중 에러 발생: {e}")
    producer.close()


app = FastAPI(lifespan=lifespan)


# ----- 유틸 함수 -----
def upload_json(path: str, data: json) -> None:
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
    logger.info(f"JSON 파일 업로드 완료. gs://{BUCKET_NAME}/{path}")


def upload_audio(path: str, data: bytes) -> None:
    """
    음성 파일 업로드 함수
    
    :param path: 업로드할 경로  
    :param data: 업로드할 음성 데이터
    """
    blob = bucket.blob(path)
    blob.upload_from_string(
        data,
        content_type="audio/mpeg"
    )
    logger.info(f"음성 파일 업로드 완료. gs://{BUCKET_NAME}/{path}")


def send_message(msg: dict) -> None:
    """
    Kafka Broker로 메시지를 전송하는 함수
    
    :param msg: 전송할 메시지
    """
    future = producer.send(TOPIC, msg)
    logger.info(f"Kafka 메시지 전송중...")
    try:
        metadata = future.get(timeout=5)
        logger.info(f"메시지 전송 완료. {metadata.topic}[{metadata.partition}]@{metadata.offset}")
    except KafkaError as e:
        logger.error(f"메시지 전송 실패: {e}")
        raise HTTPException(status_code=500, detail="메시지 전송 실패")


# ----- 엔드포인트 -----
@app.post("/submit")
async def submit_data(
    json_file: bytes = File(...),
    audio_file: Optional[bytes] = File(None)
) -> Response:
    """
    API 요청을 받아 Kafka Producer로 데이터를 전송하는 함수
    
    :param json_file: 문의 정보가 담긴 json 파일  
    :param audio_file: 문의 음성 데이터
    
    :return: 요청 결과
    """
    try:
        payload = json.loads(json_file.decode("utf-8"))
        has_audio = audio_file is not None
        logger.info(f"데이터 수신 완료 (audio={'yes' if has_audio else 'no'}), payload keys={list(payload.keys())}")

        dt = payload.get("consulting_date").replace("-","")
        consulting_id = payload.get("consulting_id")
        
        json_name = f"{dt}_consulting_{consulting_id}"
        json_path = f"{ARS_JSON_DIR if has_audio else NON_ARS_JSON_DIR}{json_name}.json"
        upload_json(json_path, payload)
        
        # 음섬 파일이 존재하는 경우(ARS)
        # 존재하지 않는 경우는 바로 위 코드로 json 파일이 업로드됨
        if has_audio:
            audio_name = f"{dt}_audio_{consulting_id}"
            audio_path = f"{AUDIO_DIR}{audio_name}.mp3"
            
            upload_audio(audio_path, audio_file)
            
            msg = {"dt": dt, "consulting_id": consulting_id}
            send_message(msg)
            
        # 응답
        return Response(status_code=204)
    except HTTPException:
        raise
    except Exception:
        logger.exception("데이터 전송 중 에러 발생.")
        raise HTTPException(status_code=500, detail="서버 에러 발생")

