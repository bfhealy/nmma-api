FROM python:3.10

ENV PORT 4000

COPY . /app

WORKDIR /app

RUN pip install -r requirements.txt

EXPOSE 4000

CMD ["make", "run_production"]
