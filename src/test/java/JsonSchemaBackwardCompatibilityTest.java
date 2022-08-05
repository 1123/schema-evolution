import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
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
    RestService schemaRegistryClient = new RestService("http://localhost:8081");

    @BeforeEach
    @SneakyThrows
    public void beforeEach() {
        try {
            schemaRegistryClient.deleteSubject(new HashMap<>(), "persons-backwards-value");
        } catch (RestClientException e) {
            if (e.getErrorCode() == 40404) {
                log.info("Could not delete subject, since it was deleted before. Continuing.");
            }
        }
        try (AdminClient kafkaAdminClient = AdminClient.create(genericProperties())) {
            kafkaAdminClient.deleteTopics(Collections.singleton(PERSONS_TOPIC));
            CreateTopicsResult topics = kafkaAdminClient.createTopics(Collections.singleton(new NewTopic(PERSONS_TOPIC,1,(short) 1)));
        }
    }

    /**
     * Change from schema version1 to version2 is backwards compatible, but from version2 to version3 is not.
     */
    @Test
    public void testCompatibility() throws RestClientException, IOException {
        schemaRegistryClient.registerSchema(SampleSchemas.SCHEMA_1, "JSON", new ArrayList<>(), "persons-backwards-value");
        schemaRegistryClient.registerSchema(SampleSchemas.SCHEMA_2, "JSON", new ArrayList<>(), "persons-backwards-value");
        RestClientException exception = Assertions.assertThrows(RestClientException.class, () -> {
            schemaRegistryClient.registerSchema(SampleSchemas.SCHEMA_3, "JSON", new ArrayList<>(), "persons-backwards-value");
        });
        assertTrue(exception.getMessage().contains("PROPERTY_REMOVED_FROM_CLOSED_CONTENT_MODEL"));
    }

    @SneakyThrows
    @Test
    public void aConsumerShouldBeAbleToConsumeWithANewerVersionOfTheSchema() {
        schemaRegistryClient.registerSchema(SampleSchemas.SCHEMA_1, "JSON", new ArrayList<>(), "persons-backwards-value");
        // Produce Data with V1
        KafkaProducer<String, PersonV1> jsonSchemaProducer = new KafkaProducer<>(producerV1Properties());
        jsonSchemaProducer.send(new ProducerRecord<>(PERSONS_TOPIC, "Joe", new PersonV1("Joe")));
        jsonSchemaProducer.send(new ProducerRecord<>(PERSONS_TOPIC, "Nobody", new PersonV1()));
        jsonSchemaProducer.close();
        // Consume Data with V1
        KafkaConsumer<String, PersonV1> jsonSchemaConsumerV1 = new KafkaConsumer<>(consumerV1Properties());
        jsonSchemaConsumerV1.subscribe(Collections.singleton(PERSONS_TOPIC));
        ConsumerRecords<String, PersonV1> resultsV1 = jsonSchemaConsumerV1.poll(Duration.ofSeconds(10));
        var iteratorV1 = resultsV1.iterator();
        assertEquals(new PersonV1("Joe"), iteratorV1.next().value());
        assertEquals(new PersonV1(), iteratorV1.next().value());
        jsonSchemaConsumerV1.close();
        // Consume Data with V2
        schemaRegistryClient.registerSchema(SampleSchemas.SCHEMA_2, "JSON", new ArrayList<>(), "persons-backwards-value");
        KafkaConsumer<String, PersonV2> jsonSchemaConsumerV2 = new KafkaConsumer<>(consumerV2Properties());
        jsonSchemaConsumerV2.subscribe(Collections.singleton(PERSONS_TOPIC));
        ConsumerRecords<String, PersonV2> resultsV2 = jsonSchemaConsumerV2.poll(Duration.ofSeconds(10));
        var iteratorV2 = resultsV2.iterator();
        assertEquals(new PersonV2("Joe", null), iteratorV2.next().value());
        assertEquals(new PersonV2(), iteratorV2.next().value());
        jsonSchemaConsumerV2.close();
    }

    @SneakyThrows
    private Properties genericProperties() {
        Properties properties = new Properties();
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream stream = loader.getResourceAsStream("local.properties");
        properties.load(stream);
        return properties;
    }

    private Properties producerV1Properties() {
        Properties properties = genericProperties();
        properties.put("key.serializer", StringSerializer.class);
        properties.put("value.serializer", KafkaJsonSchemaSerializer.class);
        properties.put("json.oneof.for.nullables", false);
        return properties;
    }

    private Properties consumerProperties() {
        Properties properties = genericProperties();
        properties.put("json.fail.unknown.properties", true);
        properties.put("key.deserializer", StringDeserializer.class);
        properties.put("value.deserializer", KafkaJsonSchemaDeserializer.class);
        properties.put("auto.offset.reset", "earliest");
        return properties;
    }

    private Properties consumerV1Properties() {
        Properties properties = consumerProperties();
        properties.put("group.id", "person-consumer-v1");
        properties.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, PersonV1.class.getName());
        return properties;
    }

    private Properties consumerV2Properties() {
        Properties properties = consumerProperties();
        properties.put("group.id", "person-consumer-v2");
        properties.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, PersonV2.class.getName());
        return properties;
    }

}



