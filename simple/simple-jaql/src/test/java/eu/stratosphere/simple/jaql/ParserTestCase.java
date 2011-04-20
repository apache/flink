package eu.stratosphere.simple.jaql;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;

import eu.stratosphere.sopremo.JsonPath;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Plan;
import eu.stratosphere.sopremo.ValueAssignment;
import eu.stratosphere.sopremo.JsonPath.ArrayCreation;

public class ParserTestCase {

	public static void assertParseResult(Operator parseResult, String jaqlScript) {
		assertParseResult(new Plan(parseResult), jaqlScript);
	}

	public static void assertParseResult(Plan expected, String jaqlScript) {
		Plan parsedPlan;
		try {
			parsedPlan = new JaqlPlanCreator().getPlan(new ByteArrayInputStream(jaqlScript.getBytes()));
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("cannot parse jaql script: " + jaqlScript + " " + e.toString());
			return;
		}
		if (parsedPlan == null && expected != null)
			Assert.fail("empty plan unexpected");

		List<Operator> expectedNodes = expected.getAllNodes();
		List<Operator> actualNodes = parsedPlan.getAllNodes();
		if (expectedNodes.size() != actualNodes.size())
			Assert.fail(String.format("%d nodes expected instead of %d", expectedNodes.size(), actualNodes.size()));

		for (int index = 0; index < expectedNodes.size(); index++) {
			if (!expectedNodes.get(index).equals(actualNodes.get(index)))
				Assert.fail(String.format("%d. node differs: %s expected instead of %s", index,
					expectedNodes.get(index), actualNodes.get(index)));
		}
	}

	public static ArrayCreation createJsonArray(Object... constants) {
		JsonPath[] elements = new JsonPath[constants.length];
		for (int index = 0; index < elements.length; index++)
			if (constants[index] instanceof JsonPath)
				elements[index] = (JsonPath) constants[index];
			else
				elements[index] = new JsonPath.Constant(constants[index]);
		return new JsonPath.ArrayCreation(elements);
	}

	public static JsonPath createObject(Object... fields) {
		if (fields.length % 2 != 0)
			throw new IllegalArgumentException();
		ValueAssignment[] assignments = new ValueAssignment[fields.length / 2];
		for (int index = 0; index < assignments.length; index++) {
			assignments[index] = new ValueAssignment(fields[2 * index].toString(), new JsonPath.Constant(
				fields[2 * index + 1]));
		}
		return new JsonPath.ObjectCreation(assignments);
	}

	public static JsonPath createPath(String... parts) {
		JsonPath lastSegment = null, firstSegment = null;
		for (int index = 0; index < parts.length; index++) {
			JsonPath segment;
			if (parts[index].equals("$"))
				segment = new JsonPath.Input(0);
			else if (parts[index].matches("[0-9]+"))
				segment = new JsonPath.Input(Integer.parseInt(parts[index]));
			else if (parts[index].matches("\\[.*\\]")) {
				if (parts[index].charAt(1) == '*')
					segment = new JsonPath.ArrayAccess();
				else if (parts[index].contains(":")) {
					int delim = parts[index].indexOf(":");
					segment = new JsonPath.ArrayAccess(
						Integer.parseInt(parts[index].substring(1, delim)),
						Integer.parseInt(parts[index].substring(delim + 1, parts[index].length() - 1)));
				} else
					segment = new JsonPath.ArrayAccess(
						Integer.parseInt(parts[index].substring(1, parts[index].length() - 1)));
			} else
				segment = new JsonPath.FieldAccess(parts[index]);
			if (lastSegment != null)
				lastSegment.setSelector(segment);
			else
				firstSegment = segment;
			lastSegment = segment;
		}
		return firstSegment;
	}
}
