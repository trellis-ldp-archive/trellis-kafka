/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.trellisldp.kafka;

import static java.lang.System.getProperty;
import static java.util.Objects.requireNonNull;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.spi.EventService.serialize;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.rdf.api.IRI;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.trellisldp.spi.Event;
import org.trellisldp.spi.EventService;

/**
 * A Kafka message producer capable of publishing messages to a Kafka cluster.
 *
 * @author acoburn
 */
public class KafkaPublisher implements EventService, AutoCloseable {

    private static final Logger LOGGER = getLogger(KafkaPublisher.class);

    private final KafkaProducer<String, String> producer;
    private final String topicName;

    /**
     * Create a new Kafka Publisher
     */
    public KafkaPublisher() {
        this(getProperty("trellis.kafka.uri"), "event");
    }

    /**
     * Create a new Kafka Publisher
     * @param brokers the broker ids
     * @param topicName the name of the topic
     */
    public KafkaPublisher(final String brokers, final String topicName) {
        requireNonNull(brokers);
        requireNonNull(topicName);

        final Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("acks", System.getProperty("kafka.acks", "all"));
        props.put("retries", System.getProperty("kafka.retries", "0"));
        props.put("batch.size", System.getProperty("kafka.batch.size", "16384"));
        props.put("linger.ms", System.getProperty("kafka.linger.ms", "1"));
        props.put("buffer.memory", System.getProperty("kafka.buffer.memory", "33554432"));
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("valud.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.topicName = topicName;
        this.producer = new KafkaProducer<>(props);
        LOGGER.info("Created Kafka producer with {}", brokers);
    }

    @Override
    public void emit(final Event event) {
        requireNonNull(event, "Cannot emit a null event!");

        serialize(event).ifPresent(message ->
            producer.send(
                new ProducerRecord<>(topicName, event.getTarget().map(IRI::getIRIString).orElse(null),
                        message)));
    }

    @Override
    public void close() throws IOException {
        LOGGER.info("Shutting down Kafka producer");
        producer.close();
    }
}
