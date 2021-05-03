#
# Build stage
#
FROM maven:3.8.1-openjdk-8 AS build
COPY pom.xml /home/app/
COPY parent-pom /home/app/parent-pom
COPY schema /home/app/schema
COPY dataflow /home/app/dataflow
RUN mvn -f /home/app/pom.xml clean package -Dmaven.test.skip=true

COPY Deployment /home/app/Deployment
COPY data /home/app/data
RUN /home/app/Deployment/commands.sh
