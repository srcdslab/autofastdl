FROM frolvlad/alpine-python3

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir --prefer-binary .

CMD ["autofastdl"]
