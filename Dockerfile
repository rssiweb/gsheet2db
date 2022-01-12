FROM python:3.8-slim
RUN apt-get update && apt-get -y install cron

COPY Pipfile /app/Pipfile
COPY Pipfile.lock /app/Pipfile.lock
WORKDIR /app
RUN pip install --upgrade pip pipenv
RUN pipenv install --system

COPY entrypoint.sh /app/entrypoint.sh

COPY crontab /etc/cron.d/crontab
RUN chmod 0644 /etc/cron.d/crontab
RUN /usr/bin/crontab /etc/cron.d/crontab
RUN touch /var/log/cron.log

COPY ./app /app
ENTRYPOINT [ "/app/entrypoint.sh" ]
