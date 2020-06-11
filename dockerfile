# Dockerfile
FROM       centos:centos7
WORKDIR    /var/app

# Update and install common packages
RUN        yum -y update
RUN        yum -y install curl tar zip wget

# Install python 3.6 and wsgi dependencies
RUN        yum -y install gcc python3 python3-libs python3devel python3-pip

# Create and configure virtualenv and uwsgi
RUN        python3.6 -m venv /var/app
RUN        useradd worker -s /bin/bash
RUN        chown -R worker /var/app

# Add application and install requirements
ADD        . /var/app
RUN        if [ -f /var/app/requirements.txt ]; then /var/app/bin/pip install -I -r /var/app/requirements.txt; fi

# Set service user
USER       worker

ENTRYPOINT ["/var/app/bin/python", "-u", "index.py"]
