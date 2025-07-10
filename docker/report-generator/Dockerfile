FROM python:3.9-slim

WORKDIR /app

COPY src/report-generator/requirements.txt ./requirements.txt
COPY src/report-generator/main.py ./main.py
COPY proto/energy.proto ./proto/
COPY generated/energy_pb2.py ./generated/

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]
