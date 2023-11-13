FROM python:3.8-slim

WORKDIR /src

COPY . /src

RUN mkdir -p /nfs/general

RUN pip install --trusted-host pypi.python.org -r requirements.txt

ENV PYTHONPATH /src

EXPOSE 5005

CMD ["python", "src/api/app.py"]