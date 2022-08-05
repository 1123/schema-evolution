public class SampleSchemas {

    /**
     * This first schema states that persons can only have a single property named "firstname", and that
     * this property is of type String. Yet the attribute is not required.
     * <p>
     * Since we set additionalProperties to false, we follow a closed content model. Alternatively one could
     * set additionalProperties to true, to allow additional the json objects to contain additional fields.
     **/

    public static String SCHEMA_ONLY_FIRSTNAME_CLOSED_CONTENT = "{" +
            "  \"$schema\": \"http://json-schema.org/draft-07/schema#\"," +
            "  \"additionalProperties\" : false," +
            "  \"properties\": {" +
            "    \"firstname\": {\"type\": \"string\"}" +
            "  }," +
            "  \"title\": \"Person V 1\"," +
            "  \"type\": \"object\"" +
            "}";

    /**
     * This second schema states that persons can have the two properties "firstname" and "lastname", and both of these
     * must be of type String. Both attributes are not required.
     * <p>
     * This second schema is less specific than the first schema, in the sense that all JSON objects valid against the first schema
     * will also validate against this second schema.
     * <p>
     * Therefore changing from the first version to the second version is a backwards compatible change.
     **/
    public static String SCHEMA_FIRST_AND_LAST_NAME_CLOSED_CONTENT = "{" +
            "  \"$schema\": \"http://json-schema.org/draft-07/schema#\"," +
            "  \"additionalProperties\" : false," +
            "  \"properties\": {" +
            "    \"firstname\": {\"type\": \"string\"}," +
            "    \"lastname\": {\"type\": \"string\"}" +
            "  }," +
            "  \"title\": \"Person V 1\"," +
            "  \"type\": \"object\"" +
            "}";

    /**
     * This third schema for Persons is more restrictive than the second schema in the sense that it does not allow
     * for the property firstname. Therefore changing from schema2 to schema3 is not backwards compatible.
     */
    public static String SCHEMA_ONLY_LAST_NAME_CLOSED_CONTENT = "{" +
            "  \"$schema\": \"http://json-schema.org/draft-07/schema#\"," +
            "  \"additionalProperties\": false," +
            "  \"properties\": {" +
            "    \"lastname\": {\"type\": \"string\"}" +
            "  }," +
            "  \"title\": \"Person V 1\"," +
            "  \"type\": \"object\"" +
            "}";

}
