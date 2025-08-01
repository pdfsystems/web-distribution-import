FROM python:3-alpine

WORKDIR /usr/src/app

COPY requirements.txt ./
COPY import.py ./
RUN pip install --no-cache-dir -r requirements.txt

CMD [ "python", "import.py" ]