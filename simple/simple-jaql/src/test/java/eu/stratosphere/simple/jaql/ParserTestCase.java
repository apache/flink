package eu.stratosphere.simple.jaql;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;

import com.ibm.jaql.lang.expr.core.Expr;

import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.PathExpression;

public class ParserTestCase {

	public static void assertParseResult(Operator expected, String jaqlScript) {
		assertParseResult(SopremoModule.valueOf("test module", expected), jaqlScript);
	}

	public static void assertParseResult(PathExpression expected, String jaqlScript) {
		QueryParser jaqlPlanCreator = new QueryParser();
		Expr parsedScript = jaqlPlanCreator.parseScript(new ByteArrayInputStream(jaqlScript.getBytes()));

		Evaluable parsedPath = jaqlPlanCreator.parsePath(parsedScript);
		Assert.assertEquals(expected, parsedPath);
	}

	public static void assertParseResult(SopremoModule expected, String jaqlScript) {
		SopremoPlan parsedPlan;
		try {
			parsedPlan = new QueryParser().getPlan(new ByteArrayInputStream(jaqlScript.getBytes()));
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("cannot parse jaql script: " + jaqlScript + " " + e.toString());
			return;
		}
		if (parsedPlan == null && expected != null)
			Assert.fail("empty plan unexpected");

		List<Operator> expectedNodes = toList(expected.getReachableNodes());
		List<Operator> actualNodes = toList(parsedPlan.getContainedOperators());
		if (expectedNodes.size() != actualNodes.size())
			Assert.fail(String.format("%d nodes expected instead of %d", expectedNodes.size(), actualNodes.size()));

		for (int index = 0; index < expectedNodes.size(); index++) {
			if (!expectedNodes.get(index).equals(actualNodes.get(index))) {
				if (!expectedNodes.get(index).getKeyTransformation().equals(actualNodes.get(index).getKeyTransformation()))
					Assert.fail(String.format("transformation of %d. node differs: %s expected instead of %s", index,
						expectedNodes.get(index).getKeyTransformation(), actualNodes.get(index).getKeyTransformation()));
				else
					Assert.fail(String.format("%d. node differs: %s expected instead of %s", index,
						expectedNodes.get(index), actualNodes.get(index)));
			}
		}
	}

	@SuppressWarnings("unchecked")
	private static List<Operator> toList(Iterable<? extends Operator> reachableNodes) {
		if(reachableNodes instanceof List)
			return (List<Operator>) reachableNodes;
		ArrayList<Operator> operatorList = new ArrayList<Operator>();
		for (Operator operator : reachableNodes) 
			operatorList.add(operator);
		return operatorList;
	}

	// TODO: elimate duplicate code -> SopremoTest
	public static EvaluableExpression createJsonArray(Object... constants) {
		EvaluableExpression[] elements = new EvaluableExpression[constants.length];
		for (int index = 0; index < elements.length; index++)
			if (constants[index] instanceof EvaluableExpression)
				elements[index] = (EvaluableExpression) constants[index];
			else
				elements[index] = new ConstantExpression(constants[index]);
		return new ArrayCreation(elements);
	}

	public static Evaluable createObject(Object... fields) {
		if (fields.length % 2 != 0)
			throw new IllegalArgumentException();
		ObjectCreation.Mapping[] assignments = new ObjectCreation.Mapping[fields.length / 2];
		for (int index = 0; index < assignments.length; index++) {
			assignments[index] = new ObjectCreation.Mapping(fields[2 * index].toString(), new ConstantExpression(fields[2 * index + 1]));
		}
		return new ObjectCreation(assignments);
	}

	public static PathExpression createPath(String... parts) {
		List<EvaluableExpression> fragments = new ArrayList<EvaluableExpression>();
		for (int index = 0; index < parts.length; index++) {
			EvaluableExpression segment;
			if (parts[index].equals("$"))
				segment = new InputSelection(0);
			else if (parts[index].matches("[0-9]+"))
				segment = new InputSelection(Integer.parseInt(parts[index]));
			else if (parts[index].matches("\\[.*\\]")) {
				if (parts[index].charAt(1) == '*')
					segment = new ArrayAccess();
				else if (parts[index].contains(":")) {
					int delim = parts[index].indexOf(":");
					segment = new ArrayAccess(Integer.parseInt(parts[index].substring(1, delim)),
						Integer.parseInt(parts[index].substring(delim + 1, parts[index].length() - 1)));
				} else
					segment = new ArrayAccess(Integer.parseInt(parts[index].substring(1, parts[index].length() - 1)));
			} else
				segment = new ObjectAccess(parts[index]);
			fragments.add(segment);
		}
		return new PathExpression(fragments);
	}
}
