FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt pyproject.toml README.md ./
COPY app ./app
COPY tools ./tools
COPY run.py ./run.py
RUN python -m pip install --no-cache-dir .

EXPOSE 8080
CMD ["python", "run.py"]
