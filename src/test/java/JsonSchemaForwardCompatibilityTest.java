import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/*
 * For educational purposes: https://www.jsonschemavalidator.net/
 * This is a great blog post: https://yokota.blog/2021/03/29/understanding-json-schema-compatibility/comment-page-1/
 */

@Slf4j
public class JsonSchemaForwardCompatibilityTest {

    private final static String PERSONS_TOPIC = "persons-forwards";
    private final static String PERSONS_SUBJECT = "persons-forwards-value";
    RestService schemaRegistryClient = new RestService("http://localhost:8081");

    /**
     * Start with an empty topic and without any schemas registered before each test case.
     */

    @BeforeEach
    @SneakyThrows
    public void beforeEach() {
        try {
            schemaRegistryClient.deleteSubject(new HashMap<>(), PERSONS_SUBJECT);
            schemaRegistryClient.updateCompatibility("FORWARD", PERSONS_SUBJECT);
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
     */
    @SneakyThrows
    @Test
    public void testCompatibility() {
        schemaRegistryClient.registerSchema(SampleSchemas.SCHEMA_FIRST_AND_LAST_NAME_CLOSED_CONTENT, "JSON", new ArrayList<>(), PERSONS_SUBJECT);
        schemaRegistryClient.registerSchema(SampleSchemas.SCHEMA_ONLY_FIRSTNAME_CLOSED_CONTENT, "JSON", new ArrayList<>(), PERSONS_SUBJECT );
        RestClientException exception = Assertions.assertThrows(RestClientException.class,
                () -> schemaRegistryClient.registerSchema(SampleSchemas.SCHEMA_ONLY_LAST_NAME_CLOSED_CONTENT, "JSON", new ArrayList<>(), PERSONS_SUBJECT)
        );
        assertTrue(exception.getMessage().contains("PROPERTY_REMOVED_FROM_CLOSED_CONTENT_MODEL"));
    }

    /**
     * In this scenario the producer evolves the schema from having a first name and a last name to only having a first name.
     * All fields in both schemas are optional
     * This is a forward compatible change, since the new schema is more restrictive than the first schema.
     *
     * Hence a consumer with the old schema will still be able to deal with the new schema.
     */

    @SneakyThrows
    @Test
    public void aConsumerShouldBeAbleToConsumeWithAnOlderVersionOfTheSchema() {
        schemaRegistryClient.registerSchema(SampleSchemas.SCHEMA_FIRST_AND_LAST_NAME_CLOSED_CONTENT, "JSON", new ArrayList<>(), PERSONS_SUBJECT);
        // Produce Data with first and last name (both may be null)
        KafkaProducer<String, PersonWithFirstAndLastName> producer1 = new KafkaProducer<>(Configuration.producerProperties());
        producer1.send(new ProducerRecord<>(PERSONS_TOPIC, "Joe", new PersonWithFirstAndLastName("Joe", "Miller")));
        producer1.send(new ProducerRecord<>(PERSONS_TOPIC, "Nobody", new PersonWithFirstAndLastName(null, null)));
        producer1.close();
        schemaRegistryClient.registerSchema(SampleSchemas.SCHEMA_ONLY_FIRSTNAME_CLOSED_CONTENT, "JSON", new ArrayList<>(), PERSONS_SUBJECT);
        KafkaProducer<String, PersonWithFirstName> producer2 = new KafkaProducer<>(Configuration.producerProperties());
        producer2.send(new ProducerRecord<>(PERSONS_TOPIC, "Nobody", new PersonWithFirstName("Sam")));
        producer2.close();
        // Consume Data with first and last name
        KafkaConsumer<String, PersonWithFirstAndLastName> oldConsumer = new KafkaConsumer<>(Configuration.consumerV2Properties());
        oldConsumer.subscribe(Collections.singleton(PERSONS_TOPIC));
        ConsumerRecords<String, PersonWithFirstAndLastName> results = oldConsumer.poll(Duration.ofSeconds(10));
        var iterator = results.iterator();
        assertEquals(new PersonWithFirstAndLastName("Joe", "Miller"), iterator.next().value());
        assertEquals(new PersonWithFirstAndLastName(), iterator.next().value());
        assertEquals(new PersonWithFirstAndLastName("Sam", null), iterator.next().value());
        oldConsumer.close();
    }


}



