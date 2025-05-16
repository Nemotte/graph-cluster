FROM openjdk:17
COPY . /app
WORKDIR /app
RUN ./mvnw clean package -DskipTests
CMD ["java", "-jar", "target/*.jar"]
