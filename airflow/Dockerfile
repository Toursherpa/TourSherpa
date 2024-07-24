FROM apache/airflow:2.9.3

USER root
RUN apt-get update && apt-get install -y vim
USER airflow
# requirements.txt 복사
COPY requirements.txt /requirements.txt

# 필요한 패키지 설치
RUN pip install -r /requirements.txt
