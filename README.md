# Understanding JSON Schema Compatibility and Deserialization with Confluent

This project gives some examples on how schema registry, producers, consumers and deserializers can be configured to deal with JSON objects versioned under JSON Schema. 

There are mainly two test cases: one for backwards compatibility and one for forwards compatibilty. 

## General Remarks

Json Schemas can follow an open content model by default. 
This means that JSON documents may include additional properties besides those 
specified in the schema and still be valid against that schema. 
One can switch to a closed content model by setting the property `additionalProperties` 
in the JSON Schema document to false.

When dealing with JSON Schema and Confluent Schema Registry, 
one should use the KafkaJsonSchemaSerializer to write data to Kafka, 
and the KafkaJsonSchemaDeserializer to read data from Kafka. 

When serializing a Java Bean with the KafkaJsonSchema Serializer, by default a closed content 
model is chosen. 
This can be adjusted by specifying the schema as an annotation on the Java Bean using the
`@io.confluent.kafka.schemaregistry.annotations.Schema` annotation.
Also the serializer will derive a type union for each field, 
the first element of the union being the null type. 
This can be avoided by setting the property `json.oneof.for.nullables` to false on the producer. 

When deserializing a Json document with the KafkaJsonSchemaDeserializer, by default the 
data is deserialized to a generic JsonNode object. 
Often, a more specific deserialization is desired. 
This can be achieved by setting the `json.value.type` on the consumer. 
When doing so, the deserializer will try to deserialize the JSON document to an instance of the specified class. 
The class will need a no-argument constructor and setter methods on each field.

Generally, it is a good idea to register schemas beforehand using a CI/CD pipeline in 
order to fail fast. 
This will allow to discover any schema compatibility issues before a new verions of an application is deployed.

## Prerequisites

The test cases in this repository expect the following environment: 

* a Kafka broker available on localhost port 9092 without encryption and without authentication. 
  When running the test cases against a different Kafka cluster, security related properties can be added to `src/main/resoruces/local.properties` . 
* Confluent schema registry running on localhost port 8081 without encryption and without authentication. 
  This can also be adjusted in `src/main/resoruces/local.properties`.  

## Backwards Compatibility Tests

The class `JsonSchemaBackwardCompatibilityTest` includes tests for showcasing backwards compatibility with Json Schema. 

The first test case gives three example schemas for a person entity under the closed content model, 
and registers those schemas with Confluent Schema registry, 
finding that the schema may involve by adding an optional field, but not by removing a field. 
This is because adding an optional field under the closed content model makes the schema less specific, 
but removing an optional field makes the schema more specific. 
A consumer with a more specific schema reading data from a producer with a less specific schema
may not be able to deal with all data, and may therefore run into an error. 

The second test case shows how the serializer and the deserializer deal with null values, and how the deserializer
can deserialize a Person JSON document of version 1 to a Java object of version 2.

## Forwards Compatibility Tests

The test case `JsonSchemaForwardCompatibilityTest` configures a topic to be forward compatible, 
and then tries differen scenarios again under the closed content model. 

It finds that removing an optional field makes the schema more specific, i.e. there is 
a smaller set of JSON documents that comply with the schema, and therefore this is an allowed change 
under the forward compatibility rules. Yet removing a field is not allowed. 

It further shows that a consumer with an old schema can deal with the removal of an optional field. 







