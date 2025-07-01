FROM python:3.9-slim

WORKDIR /app
COPY src/energy-analyzer/requirements.txt .
#COPY src/energy-analyzer/main.py .
RUN pip install -r requirements.txt
CMD ["python", "main.py"]