# Pinned to an official image. The previous base, frolvlad/alpine-python3, was
# untagged (:latest) and unmaintained, so the Python version actually shipped
# in production was whatever that image last happened to build with.
FROM python:3.14-alpine

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir --prefer-binary . \
    && adduser -D -H -u 10001 autofastdl \
    && chown -R autofastdl /app

# The process only reads game directories, writes to a temp dir and talks FTP.
USER autofastdl

CMD ["autofastdl"]
