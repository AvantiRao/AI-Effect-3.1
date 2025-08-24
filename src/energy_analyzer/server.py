import os
import time
import logging
import grpc
from concurrent import futures
from pathlib import Path
import energy_pb2, energy_pb2_grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from src.common.grpc_logging import ServerLoggingInterceptor

from generated import energy_pipeline_pb2 as exec_pb2
from generated import energy_pipeline_pb2_grpc as exec_pb2_grpc

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


class ContainerExecutorServicer(exec_pb2_grpc.ContainerExecutorServicer):
    def Execute(self, request, context):
        try:
            raw_msg = energy_pb2.GenerateResponse()
            with open(request.input_file, "rb") as f:
                raw_msg.ParseFromString(f.read())

            processed = []
            skipped = 0
            for r in raw_msg.data:
                try:
                    power = float(r.power_consumption)
                    efficiency = power / 150.0
                    status = "OK" if efficiency < 1.0 else "HIGH"
                    processed.append(energy_pb2.ProcessedEnergyReport(
                        timestamp=r.timestamp,
                        household_id=r.household_id,
                        power=power,
                        efficiency=efficiency,
                        status=status,
                        anomaly_detected=(status == "HIGH")
                    ))
                except Exception:
                    skipped += 1

            report = energy_pb2.ProcessedDataReport(
                processed=processed,
                skipped_rows=skipped
            )
           
            out_path = Path(request.output_file)
            out_path.parent.mkdir(parents=True, exist_ok=True)

            with out_path.open("wb") as f:
             f.write(report.SerializeToString())


            return exec_pb2.ExecuteResponse(
                success=True,
                message=f"Analyzed {len(processed)} rows (skipped {skipped})"
            )
        except Exception as e:
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return exec_pb2.ExecuteResponse(success=False, message=str(e))


def serve(port: int = 50052):
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4),
        interceptors=[ServerLoggingInterceptor()],
    )

    energy_pb2_grpc.add_EnergyAnalyzerServicer_to_server(EnergyAnalyzerServicer(), server)
        
    # ContainerExecutor (from energy_pipeline.proto)
    exec_pb2_grpc.add_ContainerExecutorServicer_to_server(ContainerExecutorServicer(), server)

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
