FROM debian:bullseye-slim

RUN\
 DEBIAN_FRONTEND=noninteractive apt-get -qq update < /dev/null > /dev/null &&\
 DEBIAN_FRONTEND=noninteractive apt-get install -qq curl git jq parallel < /dev/null > /dev/null

WORKDIR /app
COPY \
 *.sh \
 *.pl \
 reporter.json \
 ./

RUN ./docker-setup.sh &&\
 rm docker-setup.sh

LABEL "com.github.actions.name"="Spell Checker" \
 "com.github.actions.description"="Check repository for spelling errors" \
 "com.github.actions.icon"="edit-3" \
 "com.github.actions.color"="red" \
 "repository"="http://github.com/check-spelling/check-spelling" \
 "homepage"="https://www.check-spelling.dev/" \
 "maintainer"="Josh Soref <jsoref@noreply.users.github.com>"

ENTRYPOINT ["/app/unknown-words.sh"]
