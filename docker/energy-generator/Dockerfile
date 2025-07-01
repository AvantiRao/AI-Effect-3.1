FROM python:3.9-slim

WORKDIR /app

COPY src/energy-generator/requirements.txt .
#COPY src/energy-generator/main.py .
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "main.py"]