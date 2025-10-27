# Using this library

Import this library as a maven dependency:

```
<dependency>
    <groupId>com.biit-solutions.frustration</groupId>
    <artifactId>frustration-teamworking-organization-statistics</artifactId>
</dependency>
```

Remember to add these packages to your spring boot components:

```
@ConfigurationPropertiesScan({"com.biit.kafka.config"}, "..."})
@ComponentScan({"com.biit.kafka", "com.biit.factmanager.client", "..."})
```

If needed, add the kafka configuration settings:

```
#Kafka
spring.kafka.enabled=true
spring.kafka.topic=form
spring.kafka.nca.topic=form
spring.kafka.client.id=
spring.kafka.group.id=
spring.kafka.nca.send.topic=processedForm
spring.kafka.bootstrap-servers=PLAINTEXT://kafka.server.com:29092
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=com.biit.kafka.events.EventSerializer
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=com.biit.kafka.events.EventDeserializer
kafka.encryption.key=
```

And add the connection to the Fact Manager:

```
factmanager.server.url=http://fact-manager-backend:8080/fact-manager-backend
```

## Logging

Include the log in your `logback.xml` file

````
<logger name="com.biit.kafka.plugins.FrustrationOnTeamworkingEventsLogger" additivity="false" level="DEBUG">
    <appender-ref ref="CONSOLE"/>
    <appender-ref ref="DAILY"/>
</logger>
``` 