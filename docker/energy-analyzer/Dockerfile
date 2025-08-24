FROM python:3.9-slim
WORKDIR /app

ENV PYTHONPATH=/app/generated

COPY docker/energy-analyzer/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY proto/ ./proto/
COPY generated/energy_pb2.py ./generated/
COPY generated/energy_pb2_grpc.py ./generated/
COPY generated/energy_pipeline_pb2.py ./generated/
COPY generated/energy_pipeline_pb2_grpc.py ./generated/


EXPOSE 50052
CMD ["python","-u","-m","src.energy_analyzer.server"]

HEALTHCHECK --interval=30s --timeout=5s --retries=3 CMD \
  python -c "import grpc,sys; from grpc_health.v1 import health_pb2_grpc,health_pb2; \
c=grpc.insecure_channel('localhost:50052'); s=health_pb2_grpc.HealthStub(c); \
sys.exit(0 if s.Check(health_pb2.HealthCheckRequest()).status==1 else 1)"


