FROM python:3.9.13

# For jupyter server
EXPOSE 8888

# Dev tools
RUN apt-get update
RUN apt-get install build-essential -y
    
# Dev tools
RUN --mount=type=cache,target=/root/.cache \
    python -m pip install \
    pip==22.3.1 \
    awscli>=1.25.97 \
    jupyterlab==3.4.8 \
    ipywidgets==8.0.2
    

# ==========================
# ==== Project specific ====
# ==========================

# Project python dependencies
WORKDIR /tmp
COPY requirements.txt requirements.txt
RUN --mount=type=cache,target=/root/.cache \
    python -m pip install -r requirements.txt
