FROM python:3.8

ENV PYTHONUNBUFFERED=1

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY requirements.txt /usr/src/app/

RUN pip3 install --upgrade pip
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . /usr/src/app

ENTRYPOINT ["python3"]

CMD ["-m", "swagger_server"]
