import asyncio
import logging
import json
import os
from datetime import datetime
from typing import Optional, Tuple
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, File, Response, HTTPException
from aiokafka import AIOKafkaProducer
from kafka.errors import KafkaError

# ----- 설정 -----
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB = os.environ.get("DB")

BOOTSTRAP_SERVER    = "kafka:9093"
TOPIC               = "voc-consulting-raw"
MAX_RETRIES         = 3

# ------ 공통 클라이언트 -----
db_pool: asyncpg.pool.Pool
producer: AIOKafkaProducer

# ----- 로깅 -----
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger("submit_service")


# ----- Life cycle -----
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI Lifecycle 관리 함수
    yield 이전 코드는 서버 시작 직후 실행되고,
    yield 이후 코드는 서버 종료 직후 실행된다.

    Args:
        app (FastAPI): 실행 시킬 FastAPI 앱
    """
    global db_pool, producer
    logger.info("DB & Kafka 연결중..")
    
    # DB
    db_pool = await asyncpg.create_pool(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB
    )
    logger.info("DB 연결 성공!")

    # Kafka Producer
    for attempt in range(40):
        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVER,
                acks="all",
                enable_idempotence=True,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            await producer.start()
            logger.info("Kafka 연결 성공!")
            break
        except Exception as e:
            logger.warning(f"[{attempt+1}/40] Kafka 연결 실패, 5초 후 재시도..")
            logger.error(e)
            await asyncio.sleep(5)
    else:
        logger.error("Kafka 연결 실패. 종료되지 않지만 연결은 실패 상태.")
        producer = None
    
    yield
    
    logger.info("서버를 종료합니다…")
    try:
        await producer.flush()
    except Exception as e:
        logger.warning(f"요청 전송 중 에러 발생: {e}")
    finally:
        await producer.stop()
        await db_pool.close()


app = FastAPI(lifespan=lifespan)


# ----- 유틸 함수 -----
async def execute_query_with_rollback(
        query: str,
        kafka_msg: dict,
        data: Optional[Tuple] = None):
    """
    DB 쿼리를 실행하는 함수

    Args:
        query (str): SQL 쿼리문
        kafka_msg (dict): Kafka Broker에 전달할 메시지
        data (Optional[Tuple]): 쿼리에 필요한 데이터 (기본 값은 None)
    """
    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(query, *data) if data else conn.execute(query)
                logger.info("쿼리 실행 성공!")
                await safe_send_kafka(kafka_msg)
                logger.info("Kafka 전송 성공!")
    except Exception as e:
        logger.error(f"쿼리 또는 Kafka 전송 실패로 롤백: {e}")
        raise

async def safe_send_kafka(msg: dict):
    """
    Kafka Broker로 메시지를 전송하는 함수

    Args:
        msg (dict): 문의 정보가 담긴 데이터
    """
    for attempt in range(MAX_RETRIES):
        try:
            metadata = await producer.send_and_wait(TOPIC, msg)
            logger.info(f"메시지 전송 완료. {metadata.topic}[{metadata.partition}]@{metadata.offset}")
            return
        except KafkaError as e:
            logger.warning(f"[{attempt + 1}/{MAX_RETRIES}] Kafka 전송 실패: {e}")
            await asyncio.sleep(1)
            raise HTTPException(status_code=500, detail="메시지 전송 실패")
    raise KafkaError("Kafka 전송 재시도 실패")


# ----- 엔드포인트 -----
@app.post("/submit")
async def submit_data(json_file: bytes = File(...)) -> Response:
    """
    문의 데이터를 받아서 처리하는 함수

    Args:
        json_file (bytes): 문의 정보가 담긴 json 파일

    Returns:
        (Response): 요청 결과
    """
    try:
        payload = json.loads(json_file.decode("utf-8"))
        logger.info(f"데이터 수신 완료. payload keys={list(payload.keys())}")

        # DB 저장
        insert_sql = """
        INSERT INTO consulting (consulting_id, client_id, category_id, channel_id, consulting_datetime, turns, content)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        """
        data = (
            payload["consulting_id"],
            payload["client_id"],
            payload["category_id"],
            payload["channel_id"],
            datetime.strptime(payload["consulting_datetime"], "%Y-%m-%d %H:%M:%S"),
            payload["turns"],
            payload["content"]
        )
        
        await execute_query_with_rollback(insert_sql, payload, data)
        logger.info("문의 데이터 저장 성공")
        
        return Response(status_code=204)
    except Exception as e:
        logger.exception(f"문의 데이터 저장 중 에러 발생: {e}")
        raise HTTPException(status_code=500, detail="처리 실패")
