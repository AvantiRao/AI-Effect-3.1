import os, sys
ROOT = os.path.dirname(os.path.dirname(__file__))
GEN  = os.path.join(ROOT, "generated")
for p in (ROOT, GEN):
    if p not in sys.path:
        sys.path.insert(0, p)

import grpc
import energy_pb2 as m
import energy_pb2_grpc as g

GEN_ADDR = "localhost:50051"
AN_ADDR  = "localhost:50052"
REP_ADDR = "localhost:50053"

def main(rows: int = 20):
    # 1) Generate raw data
    with grpc.insecure_channel(GEN_ADDR) as ch:
        stub = g.EnergyGeneratorStub(ch)
        gen_resp = stub.GenerateData(m.GenerateRequest(rows=rows))

    # 2) Analyze
    with grpc.insecure_channel(AN_ADDR) as ch:
        stub = g.EnergyAnalyzerStub(ch)
        ana_resp = stub.AnalyzeData(m.AnalyzeRequest(data=gen_resp.data))

    # 3) Report
    with grpc.insecure_channel(REP_ADDR) as ch:
        stub = g.ReportGeneratorStub(ch)
        rep_resp = stub.GenerateReport(m.ReportRequest(report=ana_resp.report))

    # Tell the user where the CSV is (host path because of the bind mount)
    out_path = os.path.join(ROOT, "data", "energy_report.csv")
    print(f"Report at: {out_path}")

    # Show a quick preview
    try:
        with open(out_path, "r", encoding="utf-8") as f:
            print("---- preview ----")
            for i, line in zip(range(10), f):
                print(line.rstrip())
    except FileNotFoundError:
        # Rare: if bind mount isn’t present, show container path
        print("File not found on host. It should exist inside container at /app/data/energy_report.csv")

if __name__ == "__main__":
    main()