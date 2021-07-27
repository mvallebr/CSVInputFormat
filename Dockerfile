FROM  maven:3.8.1-jdk-11-slim

COPY . /SRC
WORKDIR /SRC

RUN mvn clean test