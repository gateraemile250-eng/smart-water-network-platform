FROM python:3.13-slim

WORKDIR /opt/project

COPY infrastructure/ml/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

ENV PYTHONPATH=/opt/project

CMD ["python"]