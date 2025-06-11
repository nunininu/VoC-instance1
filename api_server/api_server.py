import asyncio
import logging
import json
import os
from datetime import datetime, date, timedelta
from typing import Optional, Tuple, List, Dict, Any
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, File, Response, HTTPException, Query
from aiokafka import AIOKafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

# ----- 설정 -----
DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB = (
    os.environ.get("DB_HOST"),
    os.environ.get("DB_PORT"),
    os.environ.get("DB_USER"),
    os.environ.get("DB_PASSWORD"),
    os.environ.get("DB")
)
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

# ----- Kafka 유틸 함수 -----
def create_kafka_topic():
    """
    Kafka 토픽 생성
    """
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP_SERVER,
            client_id="api-server"
        )
        admin.create_topics(
            [NewTopic(name=TOPIC, num_partitions=3, replication_factor=1)]
        )
        logger.info(f"토픽 생성 완료: {TOPIC}")
    except TopicAlreadyExistsError:
        logger.warning(f"이미 존재하는 토픽: {TOPIC}")
    finally:
        admin.close()

async def safe_send_kafka(msg: dict):
    """
    Kafka Broker로 메시지 전송

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

# ----- DB 유틸 함수 -----
async def fetch_query(
    query: str,
    data: Optional[Tuple] = None
) -> list[dict]:
    """
    SELECT 쿼리 결과를 JSON serializable 형태로 반환

    Args:  
        query (str): SELECT 쿼리  
        data (Optional[Tuple]): 파라미터 값. 기본은 None

    Returns:  
        list[dict]: 결과 rows
    """
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, *data) if data else await conn.fetch(query)
        return [dict(row) for row in rows]

async def execute_query_with_rollback(
        query: str,
        kafka_msg: dict = None,
        data: Optional[Tuple] = None):
    """
    INSERT, DELETE, UPDATE 쿼리 실행

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

                if kafka_msg:
                    await safe_send_kafka(kafka_msg)
                    logger.info("Kafka 전송 성공!")
    except Exception as e:
        logger.error(f"DB/Kafka 처리 실패: {e}")
        raise

# ----- Life cycle -----
async def init_database():
    """
    데이터베이스 초기화 및 연결
    """
    global db_pool
    db_pool = await asyncpg.create_pool(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB
    )
    logger.info("DB 연결 성공!")
    
async def init_kafka_producer():
    """
    Kafka Producer 생성
    """
    global producer
    retries = 20
    
    for attempt in range(retries):
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
            logger.warning(f"[{attempt+1}/{retries}] Kafka 연결 실패, 5초 후 재시도..")
            logger.error(e)
            await asyncio.sleep(5)
    else:
        logger.error("Kafka 연결 실패. 종료되지 않지만 연결은 실패 상태.")
        producer = None
        
async def close_server():
    """
    서버 종료 시 실행
    """
    logger.info("서버를 종료합니다…")
    try:
        await producer.flush()
    except Exception as e:
        logger.warning(f"요청 전송 중 에러 발생: {e}")
    finally:
        await producer.stop()
        await db_pool.close()
    
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI Lifecycle 관리  
    yield 이전 코드는 서버 시작 직후 실행되고,  
    yield 이후 코드는 서버 종료 직후 실행된다.

    Args:  
        app (FastAPI): 실행 시킬 FastAPI 앱
    """
    await init_database()
    await init_kafka_producer()
    create_kafka_topic()

    yield

    await close_server()


# ----- FastAPI 객체 생성 -----
app = FastAPI(lifespan=lifespan)


# ----- 엔드포인트 -----
@app.post("/submit")
async def submit_data(json_file: bytes = File(...)) -> Response:
    """
    문의 데이터를 받아 DB 저장 및 Kakfa 메시지 전송

    Args:  
        json_file (bytes): 문의 정보가 담긴 json 파일

    Returns:  
        (Response): 요청 결과
    """
    try:
        payload = json.loads(json_file.decode("utf-8"))
        logger.info(f"데이터 수신 완료. payload={payload}")

        # DB 저장
        insert_sql = """
        INSERT INTO consulting (consulting_id, client_id, category_id, channel_id, consulting_datetime, turns, content)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        """
        data = (
            payload["consulting_id"], payload["client_id"], payload["category_id"],
            payload["channel_id"], datetime.strptime(payload["consulting_datetime"], "%Y-%m-%dT%H:%M:%S"),
            payload["turns"], payload["content"]
        )
        await execute_query_with_rollback(insert_sql, payload, data)
        logger.info("문의 데이터 저장 성공")
        return Response(status_code=204)
    except Exception as e:
        logger.exception(f"문의 데이터 저장 중 에러 발생: {e}")
        raise HTTPException(status_code=500, detail="처리 실패")

@app.get("/consultings")
async def get_consultings(
    category_id: int = -1,
    start_date: date = Query(default_factory=lambda: date.today() - timedelta(days=30)),
    end_date: date = Query(default_factory=date.today),
    page: int = Query(1, gt=0),
    limit: int = Query(20, gt=0)
) -> Dict[str, Any]:
    """
    문의 내역 전체 최신순으로 가져온 후 반환

    Args:  
        category_id (int): 카테고리, 기본값은 전체  
        start_date (date): 조회 시작 일자, 기본값은 최근 30일  
        end_date (date): 조회 종료 일자, 기본값은 금일  
        page (int): 페이지  
        limit (int): 페이지 당 문의 수  

    Returns:  
        (Dict[str, Any]): 카테고리 목록 및 문의 내역 리스트
    """
    category_query = "SELECT * FROM category"
    consulting_query = """
    SELECT co.consulting_id, cl.client_name, cl.client_id, ca.category_name, co.consulting_datetime
    FROM consulting as co
    JOIN client as cl ON co.client_id = cl.client_id
    JOIN category as ca ON co.category_id = ca.category_id
    WHERE co.consulting_datetime BETWEEN $1 AND $2
    """

    data = [start_date, end_date]
    offset = (page - 1) * limit

    if category_id != -1:
        consulting_query += """
            AND co.category_id = $3
        ORDER BY co.consulting_datetime DESC
        LIMIT $4 OFFSET $5
        """
        data.extend([category_id, limit, offset])
    else:
        consulting_query += """
        ORDER BY co.consulting_datetime DESC
        LIMIT $3 OFFSET $4
        """
        data.extend([limit, offset])

    category, consultings = await asyncio.gather(
        fetch_query(category_query),
        fetch_query(consulting_query, tuple(data))
    )
    category_map = {c["category_name"]: c["category_id"] for c in category}
    category_map["전체"] = -1

    return {
        "category": category_map,
        "consultings": consultings
    }

@app.get("/consulting/{consulting_id}")
async def get_consulting_detail(consulting_id: str) -> dict:
    """
    문의 내역 상세 반환

    Args:  
        consulting_id (str): 문의 고유 ID

    Returns:  
        (dict): 문의 상세 내역
    """
    query = """
    SELECT cl.client_id, cl.client_name, ca.category_name, co.consulting_datetime, co.content, ar.keywords, ar.is_negative, ar.negative_point
    FROM consulting as co
    JOIN client as cl ON co.client_id = cl.client_id
    JOIN category as ca ON co.category_id = ca.category_id
    LEFT JOIN LATERAL (
        SELECT ar.keywords, ar.is_negative, ar.negative_point
        FROM analysis_result ar
        WHERE ar.consulting_id = co.consulting_id
          AND co.analysis_status = 'SUCCESSED'
        ORDER BY ar.created_datetime DESC
        LIMIT 1
    ) ar ON true
    WHERE co.consulting_id = $1
    """
    result = await fetch_query(query, (consulting_id, ))
    logger.info(f"결과 -> {result}")
    return result[0] if result else {}

@app.get("/client")
async def get_client_with_name(client_name: str) -> List[dict]:
    """
    입력한 이름과 동일한 고객 리스트 반환

    Args:  
        client_name (str): 고객 이름

    Returns:  
        (List[dict]): 동명이인 고객 리스트
    """
    query = "SELECT * FROM client WHERE client_name = $1"
    return await fetch_query(query, (client_name, ))

@app.get("/client/{client_id}")
async def get_client_detail_with_id(
    client_id: str,
    page: int = Query(1, gt=0),
    limit: int = Query(20, gt=0)
) -> dict:
    """
    입력 또는 이름 검색에서 선택한 고객 ID로 고객 상세 내역 반환

    Args:
        client_id (str): 고객 고유 ID
        page (int): 고객 최근 문의 내역 페이지
        limit (int): 페이지 당 고객 최근 문의 내역

    Returns:
        (dict): 고객 상세 내역
    """
    client_query = """
    SELECT client_id, client_name, signup_datetime, is_terminated
    FROM client
    WHERE client_id = $1
    """
    consulting_query = """
    SELECT co.consulting_id, ca.category_name, co.consulting_datetime
    FROM consulting as co
    JOIN category as ca ON co.category_id = ca.category_id
    WHERE co.client_id = $1
    ORDER BY co.consulting_datetime DESC
    LIMIT $2 OFFSET $3
    """

    offset = (page - 1) * limit

    client_result, consultings = await asyncio.gather(
        fetch_query(client_query, (client_id, )),
        fetch_query(consulting_query, (client_id, limit, offset))
    )

    if not client_result:
        raise HTTPException(status_code=404, detail="고객을 찾을 수 없습니다.")

    client = client_result[0]
    client["consultings"] = consultings

    return client

@app.get("/report")
async def get_report() -> dict:
    """
    문의에 대한 리포트 반환
    1. 2일 전 ~ 1일 전 / 1일 전 ~ 현재 문의 수 비교
    2. 2일 전 ~ 1일 전 / 1일 전 ~ 현재 평균 부정률 비교
    3. 1일 전 ~ 현재 문의 카테고리 Top 5
    4. 1일 전 ~ 현재 키워드 Top 5

    Returns:
        (dict): 리포트
    """
    now = datetime.now()
    one_day_ago = now - timedelta(days=1)
    two_days_ago = now - timedelta(days=2)

    consulting_query = """
    SELECT
        COUNT(*) FILTER (
            WHERE consulting_datetime BETWEEN $1 AND $2
        ) AS consulting_cnt_yesterday,
        COUNT(*) FILTER (
            WHERE consulting_datetime BETWEEN $2 AND $3
        ) AS consulting_cnt_today
    FROM consulting;
    """
    avg_negative_query = """
    SELECT
        COALESCE(
            AVG(ar.negative) FILTER (
                WHERE co.consulting_datetime BETWEEN $1 AND $2
            ), 0
        ) AS avg_negative_yesterday,
        COALESCE(
            AVG(ar.negative) FILTER (
                WHERE co.consulting_datetime BETWEEN $2 AND $3
            ), 0
        ) AS avg_negative_today
    FROM analysis_result AS ar
    JOIN consulting AS co ON ar.consulting_id = co.consulting_id;
    """
    category_query = """
    SELECT
        RANK() OVER (ORDER BY COUNT(*) DESC) AS rank,
        ca.category_name AS category_name,
        COUNT(*) AS cnt
    FROM consulting AS co
    JOIN category AS ca ON co.category_id = ca.category_id
    WHERE co.consulting_datetime BETWEEN $1 AND $2
    GROUP BY ca.category_name
    ORDER BY rank
    LIMIT 5;
    """
    keywords_query = """
    SELECT
        RANK() OVER (ORDER BY COUNT(*) DESC) AS rank,
        keyword,
        COUNT(*) AS cnt
    FROM (
        SELECT TRIM(unnest(string_to_array(ar.keywords, ','))) AS keyword, co.consulting_datetime
        FROM analysis_result AS ar
        JOIN consulting AS co ON ar.consulting_id = co.consulting_id
    ) AS keyword_table
    WHERE consulting_datetime BETWEEN $1 AND $2
    GROUP BY keyword
    ORDER BY rank
    LIMIT 5;
    """

    results = await asyncio.gather(
        fetch_query(consulting_query, (two_days_ago, one_day_ago, now)),
        fetch_query(avg_negative_query, (two_days_ago, one_day_ago, now)),
        fetch_query(category_query, (one_day_ago, now)),
        fetch_query(keywords_query, (one_day_ago, now))
    )

    return {
        "consulting_cnt": results[0][0],
        "avg_negative": results[1][0],
        "top_categories": results[2],
        "top_keywords": results[3]
    }
