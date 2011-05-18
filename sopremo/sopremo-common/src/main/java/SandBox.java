import java.io.IOException;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.Arithmetic;
import eu.stratosphere.sopremo.expressions.FieldAccess;
import eu.stratosphere.sopremo.expressions.Input;
import eu.stratosphere.sopremo.expressions.Path;
import eu.stratosphere.sopremo.expressions.Transformation;
import eu.stratosphere.sopremo.expressions.ValueAssignment;
import eu.stratosphere.sopremo.expressions.Arithmetic.ArithmeticOperator;

public class SandBox {

	public static void main(String[] args) throws JsonProcessingException, IOException {
		Transformation transformation = new Transformation();
		transformation.addMapping(new ValueAssignment("target", new Arithmetic(new Path(new Input(0),
			new FieldAccess("a")), ArithmeticOperator.MULTIPLY, new Path(new Input(0), new FieldAccess("b")))));

		JsonParser parser = new JsonFactory().createJsonParser("{\"a\": 4, \"b\": 3}");
		parser.setCodec(new ObjectMapper());
		JsonNode value = parser.readValueAsTree();

		System.out.println(transformation.evaluate(value, new EvaluationContext()));

		// System.out.println(transformation);
		// String str = objectToString(transformation);
		// System.out.println(str);
		// System.out.println(stringToObject(str));
		// System.out.println(stringToObject(str).equals(transformation));
	}
}
