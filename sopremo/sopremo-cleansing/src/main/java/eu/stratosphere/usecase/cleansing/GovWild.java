package eu.stratosphere.usecase.cleansing;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.codehaus.jackson.node.TextNode;

import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.PersistenceType;
import eu.stratosphere.sopremo.Sink;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.base.BuiltinFunctions;
import eu.stratosphere.sopremo.base.Grouping;
import eu.stratosphere.sopremo.cleansing.scrubbing.BlackListRule;
import eu.stratosphere.sopremo.cleansing.scrubbing.NonNullRule;
import eu.stratosphere.sopremo.cleansing.scrubbing.PatternValidationExpression;
import eu.stratosphere.sopremo.cleansing.scrubbing.SchemaMapping;
import eu.stratosphere.sopremo.cleansing.scrubbing.Validation;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ArrayProjection;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.FunctionCall;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.pact.SopremoUtil;

public class GovWild implements PlanAssembler, PlanAssemblerDescription {
	int noSubTasks;

	String inputDir;

	String outputDir;

	@Override
	public Plan getPlan(String... args) throws IllegalArgumentException {
		noSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		inputDir = (args.length > 1 ? args[1] : "");
		outputDir = (args.length > 2 ? args[2] : "");

		Operator earmarks = new Source(PersistenceType.HDFS, String.format("%s/OriginalUsEarmark2008.json", inputDir));
//		Operator congress = joinCongres();
		Operator congress = new Source(PersistenceType.HDFS, String.format("%s/Congress.json", outputDir));

		// Validation earmarkScrub = new Validation(earmarks);
		// earmarkScrub.addRule(new TypeValidationExpression(null, new ObjectAccess("")));

		Validation congressScrub = scrubCongress(congress);

		SchemaMapping congressMapping = mapCongressPerson(congressScrub);

		Sink congressOut = new Sink(PersistenceType.HDFS, String.format("%s/CongressPerson.json", outputDir),
			congressMapping);
		SopremoPlan sopremoPlan = new SopremoPlan(congressOut);
		sopremoPlan.getContext().getFunctionRegistry().register(BuiltinFunctions.class);
		return sopremoPlan.asPactPlan();
	}

	protected SchemaMapping mapCongressPerson(Validation congressScrub) {
		ObjectCreation projection = new ObjectCreation();
		projection.addMapping("firstName", new FunctionCall("extract", new ConstantExpression(",([^ ]+)")));
		projection.addMapping("middleName", new FunctionCall("extract", new ConstantExpression(",[^ ]+ (.*)")));
		projection.addMapping("lastName", new FunctionCall("camelCase", new FunctionCall("extract",
			new ConstantExpression("(.*),"))));

		ObjectCreation birthDate = new ObjectCreation();
		birthDate
			.addMapping("year", new FunctionCall("substring", new ConstantExpression(0), new ConstantExpression(4)));
		projection.addMapping("birthDate", birthDate);

		ObjectCreation deathDate = new ObjectCreation();
		deathDate
			.addMapping("year", new FunctionCall("substring", new ConstantExpression(5), new ConstantExpression(9)));
		projection.addMapping("deathDate", deathDate);

		// ObjectCreation congressParty = new ObjectCreation();
		// congressParty.addMapping("congressNumber", 0)
		// projection.addMapping("worksFor", new ArrayCreation(null));

		SchemaMapping congressMapping = new SchemaMapping(projection, congressScrub);
		return congressMapping;
	}

	protected Validation scrubCongress(Operator congress) {
		SopremoUtil.trace();
		Validation congressScrub = new Validation(congress);
		congressScrub.addRule(new NonNullRule(new ObjectAccess("memberName")));
		congressScrub.addRule(new NonNullRule(TextNode.valueOf("none"), new ObjectAccess("biography")));
//		congressScrub.addRule(new PatternValidationExpression(Pattern.compile("(?i)[a-z]*"), new ObjectAccess(
//			"congress"), new ArrayProjection(new ObjectAccess("position"))));
//		congressScrub
//			.addRule(new BlackListRule(Arrays.asList(TextNode.valueOf("NA")), TextNode.valueOf(""), new ObjectAccess(
//				"congress"), new ArrayProjection(new ObjectAccess("position"))));
		return congressScrub;
	}

	protected Operator joinCongres() {

		 ObjectCreation congressProjection = new ObjectCreation();
		
		 congressProjection.addMapping("number", new ObjectAccess("congress"));
		 congressProjection.addMapping("state", new ObjectAccess("state"));
		 congressProjection.addMapping("position", new ObjectAccess("position"));
		 congressProjection.addMapping("party", new ObjectAccess("party"));
		
		 ObjectCreation resultProjection = new ObjectCreation();
		 resultProjection.addMapping("memberName",
		 new PathExpression(new InputSelection(0), new ArrayAccess(0), new ObjectAccess("memberName")));
		 resultProjection.addMapping("birthDeath",
		 new PathExpression(new InputSelection(0), new ArrayAccess(0), new ObjectAccess("birthDeath")));
		 resultProjection.addMapping("congresses", new FunctionCall("sort", new FunctionCall("distinct",
		 new PathExpression(new InputSelection(0), new ArrayProjection(congressProjection)))));
		 resultProjection.addMapping("biography", new PathExpression(new InputSelection(1), new ArrayAccess(0), new ObjectAccess("biography")));
		
		 Grouping congress = new Grouping(resultProjection,
		 new Source(PersistenceType.HDFS, String.format("%s/OriginalUsCongress.json", inputDir)),
		 new Source(PersistenceType.HDFS, String.format("%s/OriginalUsCongressBiography.json", inputDir)));
		
		 congress.withKeyProjection(0, new ObjectAccess("biography"));
		 congress.withKeyProjection(1, new ObjectAccess("id"));

		return congress;
	}

	public static void main(String[] args) {
		new GovWild().getPlan(args);
	}

	@Override
	public String getDescription() {
		return "Parameters: [noSubStasks] [inputDir] [outputDir]";
	}
}
