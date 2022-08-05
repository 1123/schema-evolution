import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.InputStream;
import java.util.Properties;

public class Configuration {

    @SneakyThrows
    static Properties genericProperties() {
        Properties properties = new Properties();
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream stream = loader.getResourceAsStream("local.properties");
        properties.load(stream);
        return properties;
    }

    static Properties producerProperties() {
        Properties properties = genericProperties();
        properties.put("key.serializer", StringSerializer.class);
        properties.put("value.serializer", KafkaJsonSchemaSerializer.class);
        properties.put("json.oneof.for.nullables", false);
        return properties;
    }

    private static Properties consumerProperties() {
        Properties properties = genericProperties();
        properties.put("key.deserializer", StringDeserializer.class);
        properties.put("value.deserializer", KafkaJsonSchemaDeserializer.class);
        properties.put("auto.offset.reset", "earliest");
        return properties;
    }

    static Properties consumerV1Properties() {
        Properties properties = consumerProperties();
        properties.put("group.id", "person-consumer-v1");
        properties.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, PersonWithFirstName.class.getName());
        return properties;
    }

    static Properties consumerV2Properties() {
        Properties properties = consumerProperties();
        properties.put("group.id", "person-consumer-v2");
        properties.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, PersonWithFirstAndLastName.class.getName());
        return properties;
    }

}
