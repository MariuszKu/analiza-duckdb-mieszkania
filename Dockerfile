FROM apache/airflow:latest-python3.10
ADD requirements.txt .
RUN pip3 install -r requirements.txt
