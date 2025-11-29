FROM python:latest

RUN python -m pip install aiohttp

RUN pip install numpy uproot awkward vector requests atlasopenmagic pika

COPY . .

CMD ["python", "worker.py"]
