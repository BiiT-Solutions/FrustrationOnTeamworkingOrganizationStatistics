package com.biit.kafka.plugins;

import com.biit.kafka.consumers.EventListener;
import com.biit.kafka.events.Event;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

@EnableKafka
@Configuration
@ConditionalOnExpression("${spring.kafka.enabled:false}")
public class FrustrationOnTeamworkingEventConsumerListener extends EventListener {

    public FrustrationOnTeamworkingEventConsumerListener() {
        FrustrationOnTeamworkingEventsLogger.info(this.getClass(), "Frustration On Teamworking Organization Statistics Plugin loaded!");
    }

    @Override
    @KafkaListener(topicPattern = ".*",
            clientIdPrefix = "#{'${spring.kafka.client.id}'?:T(java.util.UUID).randomUUID().toString()}",
            groupId = "#{'${spring.kafka.group.id}'?:T(java.util.UUID).randomUUID().toString()}",
            containerFactory = "templateEventListenerContainerFactory", autoStartup = "${spring.kafka.enabled:true}")
    public void eventsListener(@Payload(required = false) Event event,
                               final @Header(KafkaHeaders.OFFSET) Integer offset,
                               final @Header(KafkaHeaders.GROUP_ID) String groupId,
                               final @Header(value = KafkaHeaders.KEY, required = false) String key,
                               final @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                               final @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                               final @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp) {
        super.eventsListener(event, offset, groupId, key, partition, topic, timeStamp);
    }
}
