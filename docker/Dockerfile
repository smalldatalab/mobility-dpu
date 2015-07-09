FROM java:openjdk-8-jre
MAINTAINER Jared Sieling <jared.sieling@gmail.com>

ENV SERVER_PREFIX /opt/ohmage/mobility-dpu
 
RUN mkdir -p $SERVER_PREFIX
ADD mobility-dpu.jar $SERVER_PREFIX/
EXPOSE 8085

CMD /usr/bin/java -jar $SERVER_PREFIX/mobility-dpu.jar --spring.config.location=file:$SERVER_PREFIX/