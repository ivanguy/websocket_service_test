FROM python:slim

WORKDIR /app/

COPY requirements.txt /app/

RUN pip install -r ./requirements.txt


ENV RUN_PORT 8000
ENV REDIS_PORT=

COPY server.py /app/

CMD python server.py
