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

SERVICE_NAME = "energy.EnergyAnalyzer"


class EnergyAnalyzerServicer(energy_pb2_grpc.EnergyAnalyzerServicer):
    def AnalyzeData(self, request, context):
        processed, skipped = [], 0
        for r in request.data:
            try:
                power = float(r.power_consumption)
                efficiency = power / 150.0  # placeholder calculation
                status = "OK" if efficiency < 1.0 else "HIGH"
                processed.append(energy_pb2.ProcessedEnergyReport(
                    timestamp=r.timestamp,
                    household_id=r.household_id,
                    power=power,
                    efficiency=efficiency,
                    status=status,
                    anomaly_detected=(status == "HIGH"),
                ))
            except Exception:
                skipped += 1

        log.info(
            "AnalyzeData: received=%d processed=%d skipped=%d",
            len(request.data), len(processed), skipped,
        )

        return energy_pb2.AnalyzeResponse(
            report=energy_pb2.ProcessedDataReport(
                processed=processed,
                skipped_rows=skipped,
            )
        )


def serve(port: int = 50052):
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4),
        interceptors=[ServerLoggingInterceptor()],
    )

    energy_pb2_grpc.add_EnergyAnalyzerServicer_to_server(EnergyAnalyzerServicer(), server)

    # health service
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
