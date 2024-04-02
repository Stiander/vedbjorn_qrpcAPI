FROM python:3.10-slim-buster

LABEL IMAGE="grpc"

RUN adduser --disabled-password --gecos '' --shell /bin/bash grpc && \
    usermod -aG sudo,staff grpc && \
    apt update && apt install -y --no-install-recommends sudo && \
    echo "grpc ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers.d/10-grpc && \
    chown grpc:grpc /usr/local/bin/pip

WORKDIR /home/grpc

COPY --chown=grpc:grpc requirements.txt .

ENV PATH="/home/grpc/.local/bin:${PATH}"

USER grpc

RUN pip3 install --upgrade pip && \
    pip3 install --no-cache-dir --user -r requirements.txt

COPY --chown=grpc:grpc src/ .

RUN sudo rm -f /etc/sudoers.d/10-grpc

EXPOSE 50051
EXPOSE 443

CMD [ "python3", "-u", "main.py" ]
