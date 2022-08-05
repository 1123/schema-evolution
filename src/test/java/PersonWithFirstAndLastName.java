import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * NOTE: when sending the following entity via the JsonSchemaSerializer to Kafka, this will by default register a
 * schema with additionalProperties set to false, i.e. no additional properties will be allowed by the schema except
 * for the attributes specified in the Java class.
 * <p>
 * One can use an annotation on the class level to override this. See the Confluent docs for details.
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PersonWithFirstAndLastName {

    private String firstname;
    private String lastname;
}
