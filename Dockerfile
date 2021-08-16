FROM frolvlad/alpine-python3

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY autofastdl.py /usr/src/app/autofastdl.py

CMD ["python", "./autofastdl.py"]
