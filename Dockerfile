FROM python:3.9-slim-buster

WORKDIR /python

COPY requirements.txt requirements.txt

RUN apt-get update && apt-get install -y libpq-dev gcc

RUN pip3 install -r requirements.txt

COPY . .

CMD ["python3", "-m" , "flask", "--app", "app", "run", "--host=0.0.0.0", "--port=5000"]
