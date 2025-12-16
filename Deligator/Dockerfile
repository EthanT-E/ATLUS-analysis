FROM python:3.13-alpine

RUN python -m pip install aiohttp

RUN pip install matplotlib numpy uproot awkward vector requests atlasopenmagic pika

#RUN python -c "import sys;import os.path;from atlasopenmagic import install_from_environment;install_from_environment()"

COPY . .

CMD ["python", "./deligator.py"]
