FROM apache/airflow:2.8.1-python3.10

# Copy requirements and install them during the BUILD phase
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt
