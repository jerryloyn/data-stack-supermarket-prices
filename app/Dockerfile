FROM python:3.10-slim
COPY requirements.txt /
RUN pip install -r requirements.txt
WORKDIR /
EXPOSE 8501
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

