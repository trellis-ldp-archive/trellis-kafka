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

import static org.junit.Assert.assertEquals;
import static java.util.Collections.singleton;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.mockito.Mockito.when;

import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.simple.SimpleRDF;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.trellisldp.api.Event;
import org.trellisldp.api.EventService;
import org.trellisldp.vocabulary.AS;
import org.trellisldp.vocabulary.LDP;
import org.trellisldp.vocabulary.Trellis;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author acoburn
 */
@RunWith(MockitoJUnitRunner.class)
public class KafkaPublisherTest {

    private static final RDF rdf = new SimpleRDF();

    private final String queueName = "queue";

    private final MockProducer<String, String> producer = new MockProducer<>(true, new StringSerializer(),
            new StringSerializer());

    @Mock
    private Event mockEvent;

    @Before
    public void setUp() {
        when(mockEvent.getTarget()).thenReturn(of(rdf.createIRI("trellis:repository/resource")));
        when(mockEvent.getAgents()).thenReturn(singleton(Trellis.RepositoryAdministrator));
        when(mockEvent.getIdentifier()).thenReturn(rdf.createIRI("urn:test"));
        when(mockEvent.getTypes()).thenReturn(singleton(AS.Update));
        when(mockEvent.getTargetTypes()).thenReturn(singleton(LDP.RDFSource));
        when(mockEvent.getInbox()).thenReturn(empty());
    }

    @Test
    public void testKafka() {
        final EventService svc = new KafkaPublisher(producer, queueName);
        svc.emit(mockEvent);

        final List<ProducerRecord<String, String>> records = producer.history();
        assertEquals(1L, records.size());
        assertEquals(1L, records.stream().filter(r -> r.topic().equals(queueName)).count());
    }
}
