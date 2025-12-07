FROM python:latest

RUN python -m pip install aiohttp

RUN pip install matplotlib numpy uproot awkward vector requests atlasopenmagic pika

COPY . .

CMD ["python", "./higgs_discovery.py"]
