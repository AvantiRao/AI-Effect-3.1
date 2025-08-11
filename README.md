***AI‑Effect 3.1 Energy Services***
***1)Overview***
This repository implements a three‑stage pipeline for generating, analyzing and reporting on household energy data. The pipeline is decomposed into three standalone gRPC microservices:
1. EnergyGenerator – produces synthetic RawEnergyData records. Each record captures a timestamp, household ID, power consumption, voltage and current values.
2. EnergyAnalyzer – processes raw data into derived metrics (for example, power as a float and efficiency ratios) and flags anomalies. It returns a ProcessedDataReport which includes a list of processed records and a count of skipped rows.
3. ReportGenerator – serializes the processed report to a CSV file on disk and returns the path to the report. The services use protocol buffers defined in proto/energy.proto, and the compiled Python stubs live in generated/.
These services communicate solely via RPCs defined in the .proto file. Each service exposes a health check endpoint via the gRPC health checking API.

***2)Repository layout***
Path	Purpose
proto/energy.proto	Protocol buffer definitions for messages and gRPC services
generated/	Auto‑generated Python modules (energy_pb2.py, energy_pb2_grpc.py)
src/energy_generator/	Implementation of the EnergyGenerator gRPC service
src/energy_analyzer/	Implementation of the EnergyAnalyzer gRPC service
src/report_generator/	Implementation of the ReportGenerator gRPC service
src/common/grpc_logging.py	A gRPC server interceptor for structured logging
docker/	Dockerfiles for each service
docker-compose.yml	Defines a multi‑service environment with health checks and volume mounts
data/energy_data.csv	Example input data for local testing
tools/run_pipeline.py	Helper script to exercise the services end‑to‑end

***3)Prerequisites***
The services are developed against Python 3.9 and gRPC. You can run them locally with Docker Compose to avoid installing dependencies manually.
Ensure you have the following installed:
1. Docker – to build and run containers.
2. Docker Compose v2 – used by the provided docker-compose.yml.
3. Python 3.9+ – only required if you run the pipeline script locally outside Docker.

***4)Getting Started***
Building the services
From the project root, build the service containers:
docker-compose build
This command builds three images (generator, analyzer and reporter) defined in docker/. Each image installs its own dependencies and copies the compiled protobuf stubs from the generated/ directory.


***5)Running the Services***
To start all three services with health checks enabled:
docker-compose up
The services listen on the following ports on localhost:
Service	Port
EnergyGenerator	50051
EnergyAnalyzer	50052
ReportGenerator	50053
Output files are written to the data/ directory on the host via a bind mount.


***6)Running the Pipeline Script***
Once the containers are running, you can exercise the pipeline end‑to‑end using the helper script. It will:
1.	Make an RPC to the generator to produce a batch of raw readings.
2.	Pass those readings to the analyzer to compute derived metrics.
3.	Send the processed report to the reporter to produce a CSV.
Run the script with an optional rows argument to control how many records are generated:
python3 tools/run_pipeline.py --rows 20
The CSV report will be written to data/energy_report.csv, and the script prints a small preview of the file.


***7)Development Notes***
Proto changes – if you modify proto/energy.proto, regenerate the Python stubs using:
python -m grpc_tools.protoc \
    --python_out=generated \
    --grpc_python_out=generated \
    -I proto proto/energy.proto
Make sure to update both energy_pb2.py and energy_pb2_grpc.py in generated/.
1. Logging – the server scripts enable basic structured logging. Set LOG_LEVEL=DEBUG on a service to see detailed request logs.
2. Health checks – each service registers the gRPC health checking service. Docker Compose uses these checks to sequence service startup.
3. Data – avoid committing large datasets or generated outputs. The default .gitignore excludes .pb, .json and CSV outputs produced by previous runs.

***8)Contributions***
To contribute new features or fixes:
1.	Fork the repository and create a feature branch.
2.	Make your changes and run the pipeline locally to verify that the services still interoperate.
3.	Update documentation as needed.
4.	Open a pull request with a clear description of the changes.
License
