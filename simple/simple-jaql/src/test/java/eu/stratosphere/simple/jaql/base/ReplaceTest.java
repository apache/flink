package eu.stratosphere.simple.jaql.base;

import org.junit.Test;

import eu.stratosphere.simple.jaql.SimpleTest;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.Sink;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.base.Replace;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression.ArithmeticOperator;
import eu.stratosphere.sopremo.expressions.ConstantExpression;

public class ReplaceTest extends SimpleTest {

	@Test
	public void testSimpleReplace() {
		SopremoPlan actualPlan = parseScript("$persons = read hdfs('persons.json');" +
			"$nickNames = read hdfs('nickNames.json');" +
			"$normalizedPersons = replace $person in $persons" +
			"	on $person.firstName" +
			"	with $nickNames;" +
			"write $normalizedPersons to hdfs('normalizedPersons.json');");

		SopremoPlan expectedPlan = new SopremoPlan();
		Source persons = new Source("persons.json");
		Source nickNames = new Source("nickNames.json");
		Replace replace = new Replace().
			withInputs(persons, nickNames).
			withReplaceExpression(JsonUtil.createPath("0", "firstName"));
		Sink normalizedPersons = new Sink("normalizedPersons.json").withInputs(replace);
		expectedPlan.setSinks(normalizedPersons);

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testReplaceAll() {
		SopremoPlan actualPlan = parseScript("$persons = read hdfs('persons.json');" +
			"$languages = read hdfs('languages.json');" +
			"$normalizedPersons = replace all $person in $persons " +
			"	on $person.spokenLanguages" +
			"	with $languages;" +
			"write $normalizedPersons to hdfs('normalizedPersons.json');");

		SopremoPlan expectedPlan = new SopremoPlan();
		Source persons = new Source("persons.json");
		Source languages = new Source("languages.json");
		Replace replace = new Replace().
			withInputs(persons, languages).
			withArrayElementsReplacement(true).
			withReplaceExpression(JsonUtil.createPath("0", "spokenLanguages"));
		Sink normalizedPersons = new Sink("normalizedPersons.json").withInputs(replace);
		expectedPlan.setSinks(normalizedPersons);

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testReplaceWithDefaultValue() {
		SopremoPlan actualPlan = parseScript("$persons = read hdfs('persons.json');" +
			"$languages = read hdfs('languages.json');" +
			"$normalizedPersons = replace all $person in $persons " +
			"	on $person.spokenLanguages" +
			"	with $languages" +
			"	default 'unknown language ' + $person.spokenLanguages;" +
			"write $normalizedPersons to hdfs('normalizedPersons.json');");

		SopremoPlan expectedPlan = new SopremoPlan();
		Source persons = new Source("persons.json");
		Source languages = new Source("languages.json");
		Replace replace = new Replace().
			withInputs(persons, languages).
			withArrayElementsReplacement(true).
			withDefaultExpression(new ArithmeticExpression(new ConstantExpression("unknown language"),
				ArithmeticOperator.ADDITION, JsonUtil.createPath("0", "spokenLanguages"))).
			withReplaceExpression(JsonUtil.createPath("0", "spokenLanguages"));
		Sink normalizedPersons = new Sink("normalizedPersons.json").withInputs(replace);
		expectedPlan.setSinks(normalizedPersons);

		assertEquals(expectedPlan, actualPlan);
	}
}
