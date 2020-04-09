FROM quay.io/savkov/alpine-pandas
MAINTAINER "Sasho Savkov" <me@sasho.io>

# Create the user that will run the app
RUN adduser -D -u 1000 planchet

# Copy project requirements
COPY requirements.txt /opt/project/

# install requirements & modify the data directory
RUN pip install -r /opt/project/requirements.txt && \
    mkdir /data && chown -R planchet /data

RUN chmod -R a+w /data

# copy the project
COPY ./ /opt/project

# set working directory
WORKDIR /opt/project

# switch to non-root user
USER planchet

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--workers", "1", "--port", "5005"]
