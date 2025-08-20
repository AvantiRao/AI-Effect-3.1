import os
import time
import logging
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
log = logging.getLogger(energy_analyzer) #log = logging.getLogger(__name__)

SERVICE_NAME = "energy.EnergyAnalyzer"
shutdown_event = threading.Event()

# --- graceful shutdown ---
def handle_sigterm(*_)
    log.info("SIGTERM received, marking NOT_SERVING and shutting down..")
    shutdown_event.set()

signal.signal(signal.SIGTERM, handle_sigterm)

class EnergyAnalyzerServicer(energy_pb2_grpc.EnergyAnalyzerServicer):
    def AnalyzeData(self, request, context):
        processed, skipped = [], 0
        for r in request.data:
            try:
                power = float(r.power_consumption)
                efficiency = power / 150.0  # placeholder calculation
                status = "OK" if efficiency < 1.0 else "HIGH"
                #result = generate_raw_data(request.input_path)
                processed.append(energy_pb2.ProcessedEnergyReport(
                    timestamp=r.timestamp,
                    household_id=r.household_id,
                    power=power,
                    efficiency=efficiency,
                    status=status,
                    anomaly_detected=(status == "HIGH"),
                ))
            #except Exception:
             #   skipped += 1
        
                msg = f"Generated raw dataset at {request.output_path} with {len(data)} records"
                log.info(
                    "AnalyzeData: received=%d processed=%d skipped=%d",
                    len(request.data), len(processed), skipped,
                )

                # log.info(json.dumps({
                    #    "rpc" : "AnalyzeData",
                    #    "success" : True,
                    #    "elapsed_ms": int((time.time()-t0)*1000),
                    #    "message": msg
                   # })
        
                return energy_pb2.AnalyzeResponse(
                    report=energy_pb2.ProcessedDataReport(
                        processed=processed,
                        skipped_rows=skipped,
                    )
                    success=True,
                    message=msg,
                    code=energy_pb2.ErrorCode.UNKNOWN,  
                    data=data,
                    output_path=request.output_path
                )
            except FileNotFoundError as e:
                log.warning(json.dumps({
                    "rpc": "AnalyzeData",
                    "success": False,
                    "code": "NOT_FOUND",
                    "error": str(e)
                }))
                return energy_pb2.GenerateResponse(
                    success=False,
                    message=str(e),
                    code=energy_pb2.ErrorCode.NOT_FOUND,
                )
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(str(e))
                log.error(json.dumps({
                    "rpc": "AnalyzeData",
                    "success": False,
                    "code": "INTERNAL",
                    "error": str(e)
                }))
                return energy_pb2.GenerateResponse(
                    success=False,
                    message=str(e),
                    code=energy_pb2.ErrorCode.INTERNAL,
                )

def serve(): # def serve(port: int = 50052):
    port = os.getenv("SERVICE_PORT", "50052")
    max_workers = int(os.getenv("MAX_WORKERS", 4))
    grace = int(os.getenv("SHUTDOWN_GRACE_SECONDS", 10))
    
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
        while not shutdown_event.is_set():
            time.sleep(0.25)
        #while True:
            #time.sleep(86400)
    finally:
    #except KeyboardInterrupt:
        log.info("Stopping gRPC server gracefully...")
        health_serv.set("", health_pb2.HealthCheckResponse.NOT_SERVING)
        health_serv.set(SERVICE_NAME, health_pb2.HealthCheckResponse.NOT_SERVING)
        server.stop(grace)
        #server.stop(0)


if __name__ == "__main__":
    serve()
