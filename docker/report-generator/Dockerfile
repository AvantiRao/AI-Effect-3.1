FROM python:3.9-slim

WORKDIR /app
COPY src/report-generator/requirements.txt .
#COPY src/report-generator/main.py .
RUN pip install -r requirements.txt
CMD ["python", "main.py"]