FROM python:latest

RUN python -m pip install aiohttp

RUN pip install matplotlib numpy uproot awkward vector requests atlasopenmagic

COPY . .

EXPOSE 8000

CMD ["python", "./python_analysis.py"]
