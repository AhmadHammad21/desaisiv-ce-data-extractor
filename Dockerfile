FROM public.ecr.aws/lambda/python:3.12

# Copy files
COPY requirements.txt ./
COPY config_*.yml ./
COPY *.py ./
COPY src ./src
COPY credentials ./credentials

# Install System Packages
RUN dnf update -y && dnf install awscli -y
RUN dnf install mesa-libGL -y
RUN dnf install -y poppler-utils
RUN dnf install poppler-data
RUN dnf install fontconfig

RUN mkdir ~/.aws
RUN mv credentials ~/.aws/credentials

# Install Dependencies
RUN pip install -r requirements.txt

# RUN export CODEARTIFACT_AUTH_TOKEN=$(aws codeartifact get-authorization-token --domain broker-ce --domain-owner 983376079397  --query authorizationToken --region us-east-1 --output text) && pip config set global.extra-index-url https://aws:$CODEARTIFACT_AUTH_TOKEN@broker-ce-983376079397.d.codeartifact.us-east-1.amazonaws.com/pypi/brokercecore/simple/

# RUN pip install brokercecore==5.19

# Set Environment Variables
ARG APP_ENV=dev
ENV APP_ENV=${APP_ENV}

# Set the CMD to the handler
CMD [ "app.lambda_handler" ]
