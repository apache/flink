package eu.stratosphere.usecase.cleansing;

import static eu.stratosphere.sopremo.JsonUtil.createPath;

import java.io.File;

import eu.stratosphere.pact.testing.TestPlan;
import eu.stratosphere.sopremo.Sink;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.base.Join;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.expressions.AndExpression;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.FunctionCall;
import eu.stratosphere.sopremo.jsondatamodel.IntNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.NumericNode;
import eu.stratosphere.sopremo.jsondatamodel.TextNode;
import eu.stratosphere.sopremo.pact.CsvInputFormat;

public class SopremoDuplicateDetection {

	public static void main(String[] args) {
		// input
		File fileIn = new File("data/restaurant.csv");
		Source source = new Source(CsvInputFormat.class, "file://" + fileIn.getAbsolutePath());
		source.setParameter(CsvInputFormat.COLUMN_NAMES, new String[] { "id", "name", "address", "city", "phone",
			"type" });

		// block first 2 letters
		EvaluationExpression firstTwoLettersEqual = new ComparativeExpression(new FunctionCall("substring",
			createPath("0", "name"), new ConstantExpression(IntNode.valueOf(0)), new ConstantExpression(
				IntNode.valueOf(2))), BinaryOperator.EQUAL, new FunctionCall("substring",
			createPath("1", "name"), new ConstantExpression(IntNode.valueOf(0)), new ConstantExpression(
				IntNode.valueOf(2))));
		EvaluationExpression condition = new AndExpression(firstTwoLettersEqual);
		Join blockJoin = new Join().withInputs(source, source).withJoinCondition(condition);

		// selection(id1!=id2)
		Selection select = new Selection().withInputs(blockJoin).
			withCondition(new ComparativeExpression(createPath("0", "id"), BinaryOperator.LESS, createPath("1", "id")));

		// similarity
		Selection sim = new Selection().withInputs(select).
			withCondition(
				new ComparativeExpression(createPath("0", "name"), BinaryOperator.EQUAL, createPath("1", "name")));

		// selection phone number equality
		EvaluationExpression phone1 = new FunctionCall("replaceAll", createPath("0", "phone"),
			new ConstantExpression(new TextNode("\\W")), new ConstantExpression(
				new TextNode("")));
		EvaluationExpression phone2 = new FunctionCall("replaceAll", createPath("1", "phone"),
			new ConstantExpression(new TextNode("\\W")), new ConstantExpression(
				new TextNode("")));
		Selection phoneSimilarity = new Selection().withInputs(sim).
			withCondition(new ComparativeExpression(phone1, BinaryOperator.EQUAL, phone2));

		// project id
		Projection proj = new Projection().withInputs(phoneSimilarity).
			withValueTransformation(new ArrayCreation(createPath("0", "id"), createPath("1", "id")));

		// output
		File fileOut = new File("data/restaurant_out.json");
		final SopremoPlan sopremoPlan = new SopremoPlan(new Sink("file://" + fileOut.getAbsolutePath()).withInputs(proj));
		// SopremoUtil.trace();
		sopremoPlan.getContext().getFunctionRegistry().register(DuplicateDetectionFunctions.class);
		new TestPlan(sopremoPlan.assemblePact()).run();
	}

	public static class DuplicateDetectionFunctions {
		public static JsonNode replaceAll(JsonNode node, TextNode regex, TextNode replacement) {
			return new TextNode(((NumericNode) node).getValueAsText().replaceAll(regex.getTextValue(),
				replacement.getTextValue()));
		}
	}
}
