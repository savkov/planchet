FROM quay.io/savkov/pandas
MAINTAINER "Sasho Savkov" <sasho.savkov@babylonhealth.com>

VOLUME /data/planchet-data

# Create the user that will run the app
RUN adduser -D -u 1000 planchet

# Copy project requirements
COPY requirements.txt /opt/project/

# install requirements & modify the data directory
RUN pip install -r /opt/project/requirements.txt && \
    chown planchet /data/planchet-data

# copy the project
COPY ./ /opt/project

# set working directory
WORKDIR /opt/project

# switch to non-root user
USER planchet

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--workers", "1", "--port", "5005"]
