FROM python:3.8-slim

WORKDIR /src

COPY . /src

RUN mkdir -p /nfs/general

RUN pip install --trusted-host pypi.python.org -r requirements.txt

ENV PYTHONPATH /src

EXPOSE 8080

CMD ["python", "src/app.py"]