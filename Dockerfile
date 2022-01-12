FROM python:3.8-slim
RUN apt-get update && apt-get -y install cron

COPY Pipfile /app/Pipfile
COPY Pipfile.lock /app/Pipfile.lock
WORKDIR /app
RUN pip install --upgrade pip pipenv
RUN pipenv install --system

COPY crontab /etc/cron.d/crontab
RUN chmod 0644 /etc/cron.d/crontab
RUN /usr/bin/crontab /etc/cron.d/crontab

ENV CREDENTIALS_SERVICE_ACCOUNT='' 

COPY ./app /app
CMD ["cron", "-f"]
