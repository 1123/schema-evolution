import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/*
 * For educational purposes: https://www.jsonschemavalidator.net/
 * This is a great blog post: https://yokota.blog/2021/03/29/understanding-json-schema-compatibility/comment-page-1/
 */

@Slf4j
public class JsonSchemaBackwardCompatibilityTest {

    private final static String PERSONS_TOPIC = "persons-backwards";
    private final static String PERSONS_SUBJECT = "persons-backwards-value";
    RestService schemaRegistryClient = new RestService("http://localhost:8081");

    @BeforeEach
    @SneakyThrows
    public void beforeEach() {
        try {
            schemaRegistryClient.deleteSubject(new HashMap<>(), PERSONS_SUBJECT);
        } catch (RestClientException e) {
            if (e.getErrorCode() == 40404) {
                log.info("Could not delete subject, since it was deleted before. Continuing.");
            }
        }
        try (AdminClient kafkaAdminClient = AdminClient.create(Configuration.genericProperties())) {
            kafkaAdminClient.deleteTopics(Collections.singleton(PERSONS_TOPIC));
            CreateTopicsResult topics = kafkaAdminClient.createTopics(Collections.singleton(new NewTopic(PERSONS_TOPIC,1,(short) 1)));
        }
    }

    /**
     * Change from schema version1 to version2 is backwards compatible, but from version2 to version3 is not.
     */
    @Test
    public void testCompatibility() throws RestClientException, IOException {
        schemaRegistryClient.registerSchema(SampleSchemas.SCHEMA_ONLY_FIRSTNAME_CLOSED_CONTENT, "JSON", new ArrayList<>(), PERSONS_SUBJECT);
        schemaRegistryClient.registerSchema(SampleSchemas.SCHEMA_FIRST_AND_LAST_NAME_CLOSED_CONTENT, "JSON", new ArrayList<>(), PERSONS_SUBJECT);
        RestClientException exception = Assertions.assertThrows(RestClientException.class, () -> {
            schemaRegistryClient.registerSchema(SampleSchemas.SCHEMA_ONLY_LAST_NAME_CLOSED_CONTENT, "JSON", new ArrayList<>(), PERSONS_SUBJECT);
        });
        assertTrue(exception.getMessage().contains("PROPERTY_REMOVED_FROM_CLOSED_CONTENT_MODEL"));
    }

    @SneakyThrows
    @Test
    public void aConsumerShouldBeAbleToConsumeWithANewerVersionOfTheSchema() {
        schemaRegistryClient.registerSchema(SampleSchemas.SCHEMA_ONLY_FIRSTNAME_CLOSED_CONTENT, "JSON", new ArrayList<>(), PERSONS_SUBJECT);
        // Produce Data with V1
        KafkaProducer<String, PersonWithFirstName> jsonSchemaProducer = new KafkaProducer<>(Configuration.producerProperties());
        jsonSchemaProducer.send(new ProducerRecord<>(PERSONS_TOPIC, "Joe", new PersonWithFirstName("Joe")));
        jsonSchemaProducer.send(new ProducerRecord<>(PERSONS_TOPIC, "Nobody", new PersonWithFirstName()));
        jsonSchemaProducer.close();
        // Consume Data with V1
        KafkaConsumer<String, PersonWithFirstName> jsonSchemaConsumerV1 = new KafkaConsumer<>(Configuration.consumerV1Properties());
        jsonSchemaConsumerV1.subscribe(Collections.singleton(PERSONS_TOPIC));
        ConsumerRecords<String, PersonWithFirstName> resultsV1 = jsonSchemaConsumerV1.poll(Duration.ofSeconds(10));
        var iteratorV1 = resultsV1.iterator();
        assertEquals(new PersonWithFirstName("Joe"), iteratorV1.next().value());
        assertEquals(new PersonWithFirstName(), iteratorV1.next().value());
        jsonSchemaConsumerV1.close();
        // Consume Data with V2
        schemaRegistryClient.registerSchema(SampleSchemas.SCHEMA_FIRST_AND_LAST_NAME_CLOSED_CONTENT, "JSON", new ArrayList<>(), PERSONS_SUBJECT);
        KafkaConsumer<String, PersonWithFirstAndLastName> jsonSchemaConsumerV2 = new KafkaConsumer<>(Configuration.consumerV2Properties());
        jsonSchemaConsumerV2.subscribe(Collections.singleton(PERSONS_TOPIC));
        ConsumerRecords<String, PersonWithFirstAndLastName> resultsV2 = jsonSchemaConsumerV2.poll(Duration.ofSeconds(10));
        var iteratorV2 = resultsV2.iterator();
        assertEquals(new PersonWithFirstAndLastName("Joe", null), iteratorV2.next().value());
        assertEquals(new PersonWithFirstAndLastName(), iteratorV2.next().value());
        jsonSchemaConsumerV2.close();
    }

}



