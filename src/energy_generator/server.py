import os
import time
import logging
import grpc
from concurrent import futures
from pathlib import Path
import csv 
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

class ContainerExecutorServicer(exec_pb2_grpc.ContainerExecutorServicer):
    def Execute(self, request, context):
        """
        Reads request.input_file (a CSV) or generates synthetic records,
        writes a GenerateResponse to request.output_file,
        and returns a success/failure message.
        """
        try:
            # Determine number of rows from CSV 
            records = []
            if request.input_file and Path(request.input_file).exists():
                with open(request.input_file, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        records.append(energy_pb2.RawEnergyData(
                            timestamp=row["timestamp"],
                            household_id=row["household_id"],
                            power_consumption=row["power_consumption"],
                            voltage=row.get("voltage", "230"),
                            current=row.get("current", "5"),
                        ))
            else:
                # Fall back to synthetic data if no input file
                for i in range(10):
                    records.append(energy_pb2.RawEnergyData(
                        timestamp=f"2025-01-01T00:00:{i:02d}Z",
                        household_id=f"HH-{i%3}",
                        power_consumption=str(120 + i),
                        voltage="230",
                        current="5",
                    ))

            # Pack into a GenerateResponse and write to output_file

            gen_resp = energy_pb2.GenerateResponse(data=records)

            # ensure the /app/data (or /data) folder exists inside the container
            out_path = Path(request.output_file)
            out_path.parent.mkdir(parents=True, exist_ok=True)

            with out_path.open("wb") as f:
                f.write(gen_resp.SerializeToString())
            

            return exec_pb2.ExecuteResponse(
                success=True,
                message=f"Wrote {len(records)} raw records to {request.output_file}"
            )
        except Exception as e:
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return exec_pb2.ExecuteResponse(success=False, message=str(e))

def serve(port: int = 50051):
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4),
        interceptors=[ServerLoggingInterceptor()],
    )

    # register real service
    energy_pb2_grpc.add_EnergyGeneratorServicer_to_server(EnergyGeneratorServicer(), server)
    # ContainerExecutor (from energy_pipeline.proto)
    exec_pb2_grpc.add_ContainerExecutorServicer_to_server(ContainerExecutorServicer(), server)

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
