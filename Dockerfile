FROM python:3.7
# MSSQL dependencies
RUN wget https://packages.microsoft.com/debian/9/prod/pool/main/m/msodbcsql17/msodbcsql17_17.5.2.1-1_amd64.deb
RUN apt-get update
RUN apt-get install -y apt-utils
RUN apt-get install -y unixodbc unixodbc-dev
RUN yes | dpkg -i msodbcsql17_17.5.2.1-1_amd64.deb
# Python dependencies
RUN pip install pipenv
COPY Pipfile .
RUN pipenv install --skip-lock
COPY main.py main.py
COPY .env .env
COPY .known_hosts .known_hosts
WORKDIR /