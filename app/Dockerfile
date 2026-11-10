FROM python:3.12.7-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY agent.py .
RUN useradd -u 10001 -m agent
USER 10001
EXPOSE 8080
CMD ["python", "agent.py"]
