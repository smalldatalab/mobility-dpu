FROM java:openjdk-8-jre
MAINTAINER Jared Sieling <jared.sieling@gmail.com>

ENV APP_DIR /app
 
RUN mkdir -p $APP_DIR
ADD mobility-dpu.jar $APP_DIR/
ADD config.json $APP_DIR/
EXPOSE 8085

WORKDIR $APP_DIR
CMD java -jar "mobility-dpu.jar" -Xms256M -Xmx512M -XX:MaxMetaspaceSize=256m