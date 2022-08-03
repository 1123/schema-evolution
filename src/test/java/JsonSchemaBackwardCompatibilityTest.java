import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;

@Slf4j
public class JsonSchemaBackwardCompatibilityTest {

    private final static String PERSONS_TOPIC = "persons";
    RestService schemaRegistryClient = new RestService("http://localhost:8081");

    @SneakyThrows
    private void registerSchemaV1() {
        schemaRegistryClient.registerSchema("{" +
                "  \"$schema\": \"http://json-schema.org/draft-07/schema#\"," +
                "  \"properties\": {" +
                "    \"firstname\": {\"type\": \"string\"}" +
                "  }," +
                "  \"title\": \"Person V 1\"," +
                "  \"type\": \"object\"" +
                "}", "JSON", new ArrayList<>(), "persons-value");
    }

    @SneakyThrows
    private void registerSchemaV2() {
        schemaRegistryClient.registerSchema("{" +
                "  \"$schema\": \"http://json-schema.org/draft-07/schema#\"," +
                "  \"properties\": {" +
                "    \"firstname\": {\"type\": \"string\"}," +
                "    \"lastname\": {\"type\": \"string\"}" +
                "  }," +
                "  \"title\": \"Person V 1\"," +
                "  \"type\": \"object\"" +
                "}", "JSON", new ArrayList<>(), "persons-value");
    }

    @SneakyThrows
    private void registerSchemaV3() {
        schemaRegistryClient.registerSchema("{" +
                "  \"$schema\": \"http://json-schema.org/draft-07/schema#\"," +
                "  \"additionalProperties\": false," +
                "  \"properties\": {" +
                "    \"lastname\": {\"type\": \"string\"}" +
                "  }," +
                "  \"title\": \"Person V 1\"," +
                "  \"type\": \"object\"" +
                "}", "JSON", new ArrayList<>(), "persons-value");
    }


    @BeforeEach
    public void beforeEach() throws RestClientException, IOException {
        try {
            schemaRegistryClient.deleteSubject(new HashMap<>(), "persons-value");
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


    @Test
    public void testCompatibility() {
        registerSchemaV1();
        registerSchemaV2();
        // registerSchemaV3();
    }

    @SneakyThrows
    @Test
    public void testSerializationDeserializationWithDifferentSchemas() {
        registerSchemaV1();
        // Produce Data with V1
        KafkaProducer<String, PersonV1> jsonSchemaProducer = new KafkaProducer<>(producerV1Properties());
        jsonSchemaProducer.send(new ProducerRecord<>(PERSONS_TOPIC, "Joe", new PersonV1("Joe")));
        jsonSchemaProducer.close();
        // Consume Data with V1
        KafkaConsumer<String, PersonV1> jsonSchemaConsumerV1 = new KafkaConsumer<>(consumerV1Properties());
        jsonSchemaConsumerV1.subscribe(Collections.singleton(PERSONS_TOPIC));
        ConsumerRecords<String, PersonV1> resultsV1 = jsonSchemaConsumerV1.poll(Duration.ofSeconds(10));
        resultsV1.iterator().forEachRemaining(result -> log.info("result: " + result.value()));
        jsonSchemaConsumerV1.close();
        // Consume Data with V2
        registerSchemaV2();
        KafkaConsumer<String, PersonV1> jsonSchemaConsumerV2 = new KafkaConsumer<>(consumerV2Properties());
        jsonSchemaConsumerV2.subscribe(Collections.singleton(PERSONS_TOPIC));
        ConsumerRecords<String, PersonV1> resultsV2 = jsonSchemaConsumerV2.poll(Duration.ofSeconds(10));
        resultsV2.iterator().forEachRemaining(result -> log.info("result: " + result.value()));
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
        properties.put("json.fail.unknown.properties", true);
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
        return properties;
    }

    private Properties consumerV2Properties() {
        Properties properties = consumerProperties();
        properties.put("group.id", "person-consumer-v2");
        return properties;
    }

}

@Data
@AllArgsConstructor
class PersonV1 {

    private String firstname;

}

/**
 * NOTE: when sending the following entity via the JsonSchemaSerializer to Kafka, this will by default register a
 * schema with additionalProperties set to false, i.e. no additional properties will be allowed by the schema except
 * for the attributes specified in the Java class.
 *
 * One can use an annotation on the class level to override this. See the Confluent docs for details.
 */

@Data
@AllArgsConstructor
class PersonV2 {

    private String firstname;
    private String lastname;
}



