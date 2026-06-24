# Streamlit BI — StarRocks(MySQL 프로토콜) 조회
FROM python:3.11-slim
RUN pip install --no-cache-dir streamlit pymysql pandas
WORKDIR /app
EXPOSE 8501
CMD ["streamlit", "run", "/app/app.py", \
     "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]
