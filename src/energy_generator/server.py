import os
import time
import logging
import grpc
from concurrent import futures

from generated import energy_pb2_grpc, energy_pb2
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from src.common.grpc_logging import ServerLoggingInterceptor

# ----- logging -----
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    force=True,
)
log = logging.getLogger("energy_generator") # log = logging.getLogger(__name__)

SERVICE_NAME = "energy.EnergyGenerator"   # package.service from proto
shutdown_event = threading.Event()

# --- graceful shutdown ---
def handle_sigterm(*_)
    log.info("SIGTERM received, marking NOT_SERVING and shutting down..")
    shutdown_event.set()

signal.signal(signal.SIGTERM, handle_sigterm)

class EnergyGeneratorServicer(energy_pb2_grpc.EnergyGeneratorServicer):
    def GenerateData(self, request, context):
        try:
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
            msg = f"Generated raw dataset at {request.output_path} with {len(data)} records"
            log.info(json.dumps({
                "rpc" : "GenerateData",
                "success" : True,
                "elapsed_ms": int((time.time()-t0)*1000),
                "message": msg
            }))

            return energy_pb2.GenerateResponse(
                success=True,
                message=msg,
                code=energy_pb2.ErrorCode.UNKNOWN,  
                data=data,
                output_path=request.output_path
            )
        except FileNotFoundError as e:
            log.warning(json.dumps({
                "rpc": "GenerateData",
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
                "rpc": "GenerateData",
                "success": False,
                "code": "INTERNAL",
                "error": str(e)
            }))
            return energy_pb2.GenerateResponse(
                success=False,
                message=str(e),
                code=energy_pb2.ErrorCode.INTERNAL,
            )
        
       # return energy_pb2.GenerateResponse(data=data)

def serve(): #def serve(port: int = 50051):
    port = os.getenv("SERVICE_PORT", "50051")
    max_workers = int(os.getenv("MAX_WORKERS", 4))
    grace = int(os.getenv("SHUTDOWN_GRACE_SECONDS", 10))
    
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
