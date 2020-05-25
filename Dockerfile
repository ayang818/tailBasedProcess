#基础镜像
FROM maven:3-jdk-8-alpine

ENV HOME=/usr/local/src

COPY start.sh $HOME/start.sh

COPY target/*.jar $HOME/app.jar

WORKDIR $HOME

RUN chmod +x $HOME/start.sh

ENTRYPOINT ["/bin/bash", "$HOME/start.sh"]