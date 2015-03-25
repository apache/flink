FROM ubuntu:trusty

#requirements
RUN apt-get update; apt-get install -y curl wget supervisor openssh-server openssh-client nano

#priviledge separation directory
RUN mkdir /var/run/sshd

#install Java 7 Oracle JDK
RUN mkdir -p /usr/java/default && \
    curl -Ls 'http://download.oracle.com/otn-pub/java/jdk/7u51-b13/jdk-7u51-linux-x64.tar.gz' -H 'Cookie: oraclelicense=accept-securebackup-cookie' | \
    tar --strip-components=1 -xz -C /usr/java/default/
ENV JAVA_HOME /usr/java/default/

#Install Java Open JDK
#RUN apt-get install -y unzip openjdk-7-jre-headless
#ENV JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64/


RUN echo 'root:secret' | chpasswd

#SSH as root... probably needs to be revised for security!
RUN sed -i 's/PermitRootLogin without-password/PermitRootLogin yes/' /etc/ssh/sshd_config

EXPOSE 22

# supervisor base configuration
ADD supervisor.conf /etc/supervisor/

# default command / not overridable CMD needed for supervisord
#CMD ["supervisord", "-c", "/etc/supervisor.conf"]