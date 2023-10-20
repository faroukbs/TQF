FROM python:3.8-slim

WORKDIR /app

COPY install.txt .

RUN pip install -r install.txt

COPY ./test.py .

CMD ["python", "test.py"]