import os
import time
import logging
import grpc
from concurrent import futures

import energy_pb2, energy_pb2_grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from src.common.grpc_logging import ServerLoggingInterceptor

# ----- logging -----
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    force=True,
)
log = logging.getLogger(__name__)

SERVICE_NAME = "energy.EnergyGenerator"   # package.service from proto


class EnergyGeneratorServicer(energy_pb2_grpc.EnergyGeneratorServicer):
    def GenerateData(self, request, context):
        rows = request.rows or 10
        log.info("GenerateData: rows=%d", rows)
        data = []
        # placeholder generator
        for i in range(rows):
            data.append(energy_pb2.RawEnergyData(
                timestamp=f"2025-01-01T00:00:{i:02d}Z",
                household_id=f"HH-{i%3}",
                power_consumption=str(120 + i),
                voltage="230",
                current="5",
            ))
        return energy_pb2.GenerateResponse(data=data)


def serve(port: int = 50051):
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4),
        interceptors=[ServerLoggingInterceptor()],
    )

    # register real service
    energy_pb2_grpc.add_EnergyGeneratorServicer_to_server(EnergyGeneratorServicer(), server)

    # register health service
    health_serv = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_serv, server)
    health_serv.set("", health_pb2.HealthCheckResponse.SERVING)
    health_serv.set(SERVICE_NAME, health_pb2.HealthCheckResponse.SERVING)

    server.add_insecure_port(f"[::]:{port}")
    server.start()
    log.info("gRPC listening on :%s", port)

    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        health_serv.set("", health_pb2.HealthCheckResponse.NOT_SERVING)
        health_serv.set(SERVICE_NAME, health_pb2.HealthCheckResponse.NOT_SERVING)
        server.stop(0)


if __name__ == "__main__":
    serve()
