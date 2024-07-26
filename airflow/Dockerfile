FROM apache/airflow:2.9.3

# 작업 디렉토리 설정
WORKDIR /opt/airflow

# requirements.txt 복사 및 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && rm requirements.txt

# 필요한 디렉토리 생성 및 권한 설정
RUN mkdir -p /opt/airflow/logs /opt/airflow/dags /opt/airflow/plugins \
    && chown -R airflow: /opt/airflow/logs /opt/airflow/dags /opt/airflow/plugins

# 적절한 사용자로 실행
USER airflow

# 기본 명령 설정 (필요에 따라 변경 가능)
CMD ["airflow", "webserver"]
