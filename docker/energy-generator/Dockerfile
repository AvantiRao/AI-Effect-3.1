FROM
python:3.9-slim
FROM python:3.9-slim
WORKDIR
/app
WORKDIR /app
# Copy requirements and install dependencies
# Copy requirements and install dependencies
COPY
../../src/energy-generator/requirements.txt .
COPY ../../src/energy-generator/requirements.txt .
RUN
pip install -r requirements.txt
RUN pip install -r requirements.txt
# Source code will be mounted as volume during development
# Source code will be mounted as volume during development
CMD
"python"
CMD [ ["python"
, ,
"main.py"] ]
