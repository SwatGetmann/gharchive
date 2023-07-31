FROM python:3.9-slim-buster

EXPOSE 8050

WORKDIR /app

COPY ./requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY ./app.py app.py

CMD ["python3", "app.py"]
