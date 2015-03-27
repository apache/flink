FROM base

#add passless key to ssh
RUN ssh-keygen -f ~/.ssh/id_rsa -t rsa -N ''
RUN cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/*

##Flink 0.8.1 Installation
###Download:
RUN mkdir ~/downloads && cd ~/downloads && \
    wget -q -O - http://mirrors.advancedhosters.com/apache/flink/flink-0.8.1/flink-0.8.1-bin-hadoop2.tgz | tar -zxvf - -C /usr/local/
RUN cd /usr/local && ln -s ./flink-0.8.1 flink

ENV FLINK_HOME /usr/local/flink
ENV PATH $PATH:$FLINK_HOME/bin

#config files (template)
ADD conf/flink-conf.yaml /usr/local/flink/conf/

ADD config-flink.sh /usr/local/flink/bin/
RUN chmod +x /usr/local/flink/bin/config-flink.sh

EXPOSE 6123
EXPOSE 22

CMD ["/usr/local/flink/bin/config-flink.sh", "taskmanager"]
