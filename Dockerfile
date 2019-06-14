FROM gettyimages/spark:2.4.1-hadoop-3.0

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && \
    apt-get -yq install \
        libpq5 \
        zip \
        && \
    rm -rf /var/lib/apt/lists/*

COPY . /srv/spark-stat-analyzer

WORKDIR /srv/spark-stat-analyzer

RUN set -xe && \
    buildDeps="libpq-dev python3-dev build-essential" && \
    apt-get update && \
    apt-get -yq install $buildDeps && \
    pip3 install -r requirements.txt && \
    apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false -o APT::AutoRemove::SuggestsImportant=false $buildDeps && \
    rm -rf /var/lib/apt/lists/*

RUN cp config.py.docker config.py && rm config.py.dist && rm config.py.docker
RUN zip -r spark-stat-analyzer.zip analyzers includes
RUN cp /usr/spark-2.4.1/conf/log4j.properties.template /usr/spark-2.4.1/conf/log4j.properties
RUN sed -i 's/INFO, console/WARN, console/g' /usr/spark-2.4.1/conf/log4j.properties

CMD ["bash"]
