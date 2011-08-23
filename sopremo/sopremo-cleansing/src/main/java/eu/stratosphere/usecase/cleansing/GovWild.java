package eu.stratosphere.usecase.cleansing;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.TextNode;

import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.PersistenceType;
import eu.stratosphere.sopremo.Sink;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.base.BuiltinFunctions;
import eu.stratosphere.sopremo.base.Join;
import eu.stratosphere.sopremo.cleansing.scrubbing.BlackListRule;
import eu.stratosphere.sopremo.cleansing.scrubbing.PatternValidationExpression;
import eu.stratosphere.sopremo.cleansing.scrubbing.SchemaMapping;
import eu.stratosphere.sopremo.cleansing.scrubbing.Validation;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.FunctionCall;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.PathExpression;

public class GovWild implements PlanAssembler, PlanAssemblerDescription {
	@Override
	public Plan getPlan(String... args) throws IllegalArgumentException {
		int noSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String inputDir = (args.length > 1 ? args[1] : "");
		String outputDir = (args.length > 2 ? args[2] : "");

		Operator earmarks = new Source(PersistenceType.HDFS, String.format("%s/OriginalUsEarmark2008.json", inputDir));
		Operator congress = readCongress(inputDir);

		// Validation earmarkScrub = new Validation(earmarks);
		// earmarkScrub.addRule(new TypeValidationExpression(null, new ObjectAccess("")));

		Validation congressScrub = scrubCongress(congress);

		SchemaMapping congressMapping = mapCongressPerson(congressScrub);

		Sink congressOut = new Sink(PersistenceType.HDFS, String.format("%s/CleanEarmarks.json", outputDir),
			congressMapping);
		return new SopremoPlan(congressOut).asPactPlan();
	}

	protected SchemaMapping mapCongressPerson(Validation congressScrub) {
		ObjectCreation projection = new ObjectCreation();
		projection.addMapping("firstName", projection);
		projection.addMapping("middleName", projection);
		projection.addMapping("lastName", projection);
		projection.addMapping("party", projection);

		ObjectCreation birthDate = new ObjectCreation();
		birthDate
			.addMapping("year", new FunctionCall("substring", new ConstantExpression(0), new ConstantExpression(4)));
		projection.addMapping("birthDate", birthDate);

		ObjectCreation deathDate = new ObjectCreation();
		birthDate
			.addMapping("year", new FunctionCall("substring", new ConstantExpression(5), new ConstantExpression(9)));
		projection.addMapping("deathDate", deathDate);

		SchemaMapping congressMapping = new SchemaMapping(projection, congressScrub);
		return congressMapping;
	}

	protected Validation scrubCongress(Operator congress) {
		Validation congressScrub = new Validation(congress);
		congressScrub.addRule(new PatternValidationExpression(Pattern.compile("(?i)[a-z]*"), "position"));
		congressScrub
			.addRule(new BlackListRule(Arrays.asList(TextNode.valueOf("NA")), TextNode.valueOf(""), "position"));
		return congressScrub;
	}

	protected Operator readCongress(String inputDir) {
		ObjectCreation joinProjection = new ObjectCreation();
		joinProjection.addMapping(new ObjectCreation.CopyFields(new InputSelection(0)));
		joinProjection
			.addMapping("biography", new PathExpression(new InputSelection(1), new ObjectAccess("biography")));
		Operator congress = new Join(
			joinProjection,
			new ComparativeExpression(new PathExpression(new InputSelection(0), new ObjectAccess("biography")),
				BinaryOperator.EQUAL, new PathExpression(new InputSelection(1), new ObjectAccess("id"))),
			new Source(PersistenceType.HDFS, String.format("%s/OriginalUsCongress.json", inputDir)),
			new Source(PersistenceType.HDFS, String.format("%s/OriginalUsCongressBiography.json", inputDir)));
		return congress;
	}

	@Override
	public String getDescription() {
		return "Parameters: [noSubStasks] [inputDir] [outputDir]";
	}
}
