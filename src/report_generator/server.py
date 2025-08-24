import os
import time
import csv
import logging
from pathlib import Path
import grpc
from concurrent import futures
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

OUT_DIR = Path("/app/data")
OUT_DIR.mkdir(parents=True, exist_ok=True)

SERVICE_NAME = "energy.ReportGenerator"


class ReportGeneratorServicer(energy_pb2_grpc.ReportGeneratorServicer):
    def GenerateReport(self, request, context):
        report = request.report  # ProcessedDataReport
        out_csv = OUT_DIR / "energy_report.csv"

        with out_csv.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["timestamp", "household_id", "power",
                        "efficiency", "status", "anomaly_detected"])
            for rec in report.processed:
                w.writerow([
                    rec.timestamp, rec.household_id, rec.power,
                    rec.efficiency, rec.status, rec.anomaly_detected
                ])

        log.info("GenerateReport: wrote %s", out_csv)
        # Returning path in html_path field (matches proto)
        return energy_pb2.ReportResponse(html_path=str(out_csv))

class ContainerExecutorServicer(exec_pb2_grpc.ContainerExecutorServicer):
    def Execute(self, request, context):
        try:
            report_data = energy_pb2.ProcessedDataReport()
            with open(request.input_file, "rb") as f:
                report_data.ParseFromString(f.read())

            out_path = Path(request.output_file)
            out_path.parent.mkdir(parents=True, exist_ok=True)

            with out_path.open("w", newline="") as f:
                writer = csv.writer(f)

                writer.writerow(["timestamp","household_id","power",
                                 "efficiency","status","anomaly_detected"])
                for rec in report_data.processed:
                    writer.writerow([
                        rec.timestamp, rec.household_id, rec.power,
                        rec.efficiency, rec.status, rec.anomaly_detected
                    ])
            return exec_pb2.ExecuteResponse(
                success=True,
                message=f"Wrote report to {request.output_file}"
            )
        except Exception as e:
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return exec_pb2.ExecuteResponse(success=False, message=str(e))


def serve(port: int = 50053):
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4),
        interceptors=[ServerLoggingInterceptor()],
    )

    energy_pb2_grpc.add_ReportGeneratorServicer_to_server(ReportGeneratorServicer(), server)    
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
