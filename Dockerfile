FROM python:3.6

ENV PYTHONUNBUFFERED 1

RUN pip install --upgrade pip

RUN apt-get update
RUN apt-get install -y vim apt-transport-https unixodbc-dev gcc nginx cron

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/9/prod.list > /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17

COPY ["nginx/nginx.conf", "/etc/nginx/nginx.conf"]
EXPOSE 8080

WORKDIR /app

COPY ["Control Center", "/app"]
COPY ["start-cron.sh", "/app"]
RUN pip install -r requirements.txt

COPY ["crontab/crontab", "/etc/cron.d/crontab"]
RUN chmod 0644 /etc/cron.d/crontab

RUN mkdir -p /var/log/docker/svc-bizops-controls
RUN touch /var/log/docker/svc-bizops-controls/svc-bizops-controls-execution.log
RUN touch /var/log/docker/svc-bizops-controls/svc-bizops-controls-inserts.log
RUN touch /var/log/docker/svc-bizops-controls/svc-bizops-controls-failedaudits.log

RUN chmod +x /app/start-cron.sh

CMD /app/start-cron.sh && touch /etc/cron.d/crontab && nginx -g 'daemon off;'
