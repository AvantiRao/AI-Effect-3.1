import os
import time
import csv
import logging
from pathlib import Path
import grpc
from concurrent import futures

#import energy_pb2, energy_pb2_grpc
from generated import energy_pb2_grpc, energy_pb2
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from src.common.grpc_logging import ServerLoggingInterceptor

# ----- logging -----
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    force=True,
)
log = logging.getLogger(report_generator) #log = logging.getLogger(__name__)

OUT_DIR = Path("/app/data")
OUT_DIR.mkdir(parents=True, exist_ok=True)

SERVICE_NAME = "energy.ReportGenerator"
shutdown_event = threading.Event()

# --- graceful shutdown ---
def handle_sigterm(*_)
    log.info("SIGTERM received, marking NOT_SERVING and shutting down..")
    shutdown_event.set()

signal.signal(signal.SIGTERM, handle_sigterm)


class ReportGeneratorServicer(energy_pb2_grpc.ReportGeneratorServicer):
    def GenerateReport(self, request, context):
        try:
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
            return energy_pb2.ReportResponse( #return energy_pb2.ReportResponse(html_path=str(out_csv))
                html_path=str(out_csv),          
                success=True,                
                message="Report generated successfully",  
                code=energy_pb2.ErrorCode.UNKNOWN 
            )
        except Exception as e:
            return energy_pb2.ReportResponse(
                html_path="",
                success=False,
                message=f"Report generation failed: {str(e)}",
                code=energy_pb2.ErrorCode.INTERNAL
            )
        #except FileNotFoundError as e:
        #    log.warning(json.dumps({
        #        "rpc": "GenerateData",
        #        "success": False,
        #        "code": "NOT_FOUND",
        #        "error": str(e)
        #    }))
        #    return energy_pb2.GenerateResponse(
        #        success=False,
        #        message=str(e),
        #        code=energy_pb2.ErrorCode.NOT_FOUND,
        #    )
       # except Exception as e:
           # context.set_code(grpc.StatusCode.INTERNAL)
           # context.set_details(str(e))
           # log.error(json.dumps({
           #     "rpc": "GenerateData",
           #     "success": False,
           #     "code": "INTERNAL",
           #     "error": str(e)
           # }))
           # return energy_pb2.GenerateResponse(
           #     success=False,
           #     message=str(e),
           #     code=energy_pb2.ErrorCode.INTERNAL,
           # )

def serve(): #def serve(port: int = 50053):
    port = os.getenv("SERVICE_PORT", "50053")
    max_workers = int(os.getenv("MAX_WORKERS", 4))
    grace = int(os.getenv("SHUTDOWN_GRACE_SECONDS", 10))
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4),
        interceptors=[ServerLoggingInterceptor()],
    )

    energy_pb2_grpc.add_ReportGeneratorServicer_to_server(ReportGeneratorServicer(), server)

    # health service
    health_serv = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_serv, server)
    health_serv.set("", health_pb2.HealthCheckResponse.SERVING)
    health_serv.set(SERVICE_NAME, health_pb2.HealthCheckResponse.SERVING)

    server.add_insecure_port(f"[::]:{port}")
    server.start()
    log.info("gRPC listening on :%s", port)

    try:
        while not shutdown_event.is_set():
            time.sleep(0.25)
        #while True:
         #   time.sleep(86400)
    finally:
    #except KeyboardInterrupt:
        log.info("Stopping gRPC server gracefully...")
        health_serv.set("", health_pb2.HealthCheckResponse.NOT_SERVING)
        health_serv.set(SERVICE_NAME, health_pb2.HealthCheckResponse.NOT_SERVING)
        server.stop(grace)
        #server.stop(0)


if __name__ == "__main__":
    serve()
