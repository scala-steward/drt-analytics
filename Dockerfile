FROM openjdk:11-jre-slim-buster as stage0
LABEL snp-multi-stage="intermediate"
LABEL snp-multi-stage-id="be4b8202-ee96-4be6-8572-d74691fc6377"
WORKDIR /opt/docker
COPY target/docker/stage/2/opt /2/opt
COPY target/docker/stage/4/opt /4/opt
USER root
RUN ["chmod", "-R", "u=rX,g=rX", "/2/opt/docker"]
RUN ["chmod", "-R", "u=rX,g=rX", "/4/opt/docker"]
RUN ["chmod", "u+x,g+x", "/4/opt/docker/bin/drt-analytics"]

FROM openjdk:11-jre-slim-buster as mainstage
USER root
RUN id -u drt 1>/dev/null 2>&1 || (( getent group 0 1>/dev/null 2>&1 || ( type groupadd 1>/dev/null 2>&1 && groupadd -g 0 root || addgroup -g 0 -S root )) && ( type useradd 1>/dev/null 2>&1 && useradd --system --create-home --uid 1001 --gid 0 drt || adduser -S -u 1001 -G root drt ))
WORKDIR /opt/docker
COPY --from=stage0 --chown=drt:root /2/opt/docker /opt/docker
COPY --from=stage0 --chown=drt:root /4/opt/docker /opt/docker

RUN mkdir -p /var/data
RUN chown 1001:1001 -R /var/data

RUN mkdir -p /var/data/logs
RUN chown 1001:1001 -R /var/data/logs

RUN apt-get update
RUN apt-get install -y openssh-client ca-certificates curl

RUN mkdir -p /etc/drt
RUN curl https://truststore.pki.rds.amazonaws.com/eu-west-2/eu-west-2-bundle.pem > /etc/drt/eu-west-2-bundle.pem
RUN openssl x509 -outform der -in /etc/drt/eu-west-2-bundle.pem -out /etc/drt/certificate.der

RUN keytool -noprompt -storepass changeit -import -alias rds-root -keystore $JAVA_HOME/lib/security/cacerts -file /etc/drt/certificate.der

USER 1001:0
ENTRYPOINT ["/opt/docker/bin/drt-analytics"]
CMD []
