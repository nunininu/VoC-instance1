FROM python:3.12-slim

# 기본 포트 설정 (docker-compose에서 override)
ENV APP_PORT=8888

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

COPY wait-for-it.sh /wait-for-it.sh
RUN chmod +x /wait-for-it.sh

CMD sh -c "/wait-for-it.sh kafka:9093 -t 120 -- uvicorn api_server:app --host 0.0.0.0 --port $APP_PORT --reload"
