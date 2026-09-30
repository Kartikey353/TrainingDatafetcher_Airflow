# Bump the tag here to upgrade Airflow. Multi-arch image, runs natively on Apple silicon.
FROM apache/airflow:3.0.6

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
