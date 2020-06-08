FROM instructure/node:10
MAINTAINER Instructure

ENV DATADIR /var/lib/kinesalite

USER root
RUN mkdir $DATADIR && chown docker:docker $DATADIR
USER docker

RUN npm install kinesalite@3.3.1

EXPOSE 4567
VOLUME $DATADIR

ENTRYPOINT ["/tini", "--", "/usr/src/app/node_modules/kinesalite/cli.js", "--path", "/var/lib/kinesalite"]