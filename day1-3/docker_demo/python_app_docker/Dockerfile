FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt
COPY . /app
COPY data ./data
ENTRYPOINT ["python3", "./app_refactored_solution.py"]