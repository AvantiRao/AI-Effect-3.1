FROM python:3.9-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY ../../src/energy-generator/requirements.txt .

RUN pip install -r requirements.txt

# Source code will be mounted as volume during development

CMD ["python", "main.py"]
