package eu.stratosphere.usecase.cleansing;

import java.util.regex.Pattern;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.IntNode;
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
import eu.stratosphere.sopremo.cleansing.scrubbing.NonNullRule;
import eu.stratosphere.sopremo.cleansing.scrubbing.PatternValidationExpression;
import eu.stratosphere.sopremo.cleansing.scrubbing.RangeRule;
import eu.stratosphere.sopremo.cleansing.scrubbing.SchemaMapping;
import eu.stratosphere.sopremo.cleansing.scrubbing.Validation;
import eu.stratosphere.sopremo.expressions.AggregationExpression;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ArrayProjection;
import eu.stratosphere.sopremo.expressions.BatchAggregationExpression;
import eu.stratosphere.sopremo.expressions.CoerceExpression;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
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
		Operator scrubbedEarmark = scrubEarmark(earmarks);
		Operator earmarkFunds = mapEarmarkFund(scrubbedEarmark);
		// Operator congress = joinCongres();
//		Sink congressOut = cleanCongress();

		Sink earmarkFundOut = new Sink(PersistenceType.HDFS, String.format("%s/EarmarkFund.json", outputDir),
			earmarkFunds);
		SopremoPlan sopremoPlan = new SopremoPlan(earmarkFundOut);
		sopremoPlan.getContext().getFunctionRegistry().register(BuiltinFunctions.class);
		sopremoPlan.getContext().getFunctionRegistry()
			.register(eu.stratosphere.usecase.cleansing.CleansFunctions.class);
		return sopremoPlan.asPactPlan();
	}

	protected Sink cleanCongress() {
		Operator congress = new Source(PersistenceType.HDFS, String.format("%s/Congress.json", outputDir));
		Operator congressScrub = scrubCongress(congress);
		Operator congressMapping = mapCongressPerson(congressScrub);
		Sink congressOut = new Sink(PersistenceType.HDFS, String.format("%s/CongressPerson.json", outputDir),
			congressMapping);
		return congressOut;
	}

	protected Operator mapCongressPerson(Operator congressScrub) {
		ObjectCreation projection = new ObjectCreation();
		projection.addMapping("firstName",
			new FunctionCall("extract", new ObjectAccess("memberName"), new ConstantExpression(", ([^ ]+)")));
		projection.addMapping("middleName",
			new FunctionCall("extract", new ObjectAccess("memberName"), new ConstantExpression(", [^ ]+ (.*)")));
		projection.addMapping("lastName",
			new FunctionCall("camelCase",
				new FunctionCall("extract", new ObjectAccess("memberName"), new ConstantExpression("(.*),"))));

		ObjectCreation birthDate = new ObjectCreation();
		birthDate.addMapping("year",
			new FunctionCall("extract", new ObjectAccess("birthDeath"), new ConstantExpression("([0-9]{0,4})-")));
		projection.addMapping("birthDate", birthDate);

		ObjectCreation deathDate = new ObjectCreation();
		deathDate.addMapping("year",
			new FunctionCall("extract", new ObjectAccess("birthDeath"), new ConstantExpression("-([0-9]{0,4})")));
		projection.addMapping("deathDate", deathDate);

		ObjectCreation partyProjection = new ObjectCreation();
		partyProjection.addMapping("legalEntity", new PathExpression(BuiltinFunctions.FIRST.asExpression(),
			new ObjectAccess("party")));
		partyProjection.addMapping("startYear", coerce(IntNode.class, new AggregationExpression(CleansFunctions.MIN, new FunctionCall(
			"extract", new ObjectAccess("number"), new ConstantExpression("\\(([0-9]{0,4})-")))));
		partyProjection.addMapping("endYear",  coerce(IntNode.class,new AggregationExpression(CleansFunctions.MAX, new FunctionCall(
			"extract", new ObjectAccess("number"), new ConstantExpression("-([0-9]{0,4})")))));
		partyProjection.addMapping("congresses",  new ArrayProjection(coerce(IntNode.class, new FunctionCall("extract", new ObjectAccess(
			"number"), new ConstantExpression("([0-9]+)\\(")))));
		AggExpression parties = new AggExpression(new ObjectAccess("party"), partyProjection);

		ObjectCreation stateProjections = new ObjectCreation();
		stateProjections.addMapping("legalEntity", new PathExpression(BuiltinFunctions.FIRST.asExpression(),
			new ObjectAccess("state")));
		stateProjections.addMapping("positions", new FunctionCall("distinct", new AggregationExpression(
			CleansFunctions.ALL, new ObjectAccess("position"))));
		stateProjections.addMapping("startYear", coerce(IntNode.class,new AggregationExpression(CleansFunctions.MIN, new FunctionCall(
			"extract", new ObjectAccess("number"), new ConstantExpression("\\(([0-9]{0,4})-")))));
		stateProjections.addMapping("endYear", coerce(IntNode.class,new AggregationExpression(CleansFunctions.MAX, new FunctionCall(
			"extract", new ObjectAccess("number"), new ConstantExpression("-([0-9]{0,4})")))));
		stateProjections.addMapping("congresses", new AggregationExpression(CleansFunctions.ALL, coerce(IntNode.class,new FunctionCall(
			"extract", new ObjectAccess("number"), new ConstantExpression("([0-9]+)\\(")))));
		AggExpression congresses = new AggExpression(new ObjectAccess("state"), stateProjections);

		projection.addMapping("worksFor", new PathExpression(new ObjectAccess("congresses"), new FunctionCall(
			"unionAll", parties, congresses)));

		SchemaMapping congressMapping = new SchemaMapping(projection, congressScrub);
		return congressMapping;
	}

	private EvaluationExpression coerce(Class<? extends JsonNode> type, EvaluationExpression expression) {
		return new PathExpression(expression, new CoerceExpression(type));
//		return expression;
	}

	protected Operator scrubEarmark(Operator earmarks) {
		Validation validation = new Validation(earmarks);

//		validation.addRule(new RangeRule(new ObjectAccess("memberName")));
		
		return validation;
	}
	
	protected Operator mapEarmarkFund(Operator scrubbedEarmarks) {
		ObjectCreation fundProjection = new ObjectCreation();

		final BatchAggregationExpression batch = new BatchAggregationExpression();
		fundProjection.addMapping("id", new PathExpression( batch.add(CleansFunctions.FIRST), new ObjectAccess("id")));
		fundProjection.addMapping("amount", batch.add(CleansFunctions.ALL, new ObjectAccess("amount")));
		fundProjection.addMapping("currency", new ConstantExpression("dollar"));
		ObjectCreation date = new ObjectCreation();
		date.addMapping("year", new PathExpression(batch.add(CleansFunctions.FIRST), new ObjectAccess("enactedYear")));
		fundProjection.addMapping("date", date);
		fundProjection.addMapping("subject", new PathExpression( batch.add(CleansFunctions.FIRST), new ObjectAccess("shortDescription")));
				
		Grouping grouping = new Grouping(fundProjection, scrubbedEarmarks);
		grouping.withKeyProjection(new ObjectAccess("earmarkId"));
		return grouping;
	}

	protected Operator scrubCongress(Operator congress) {
		SopremoUtil.trace();
		Validation congressScrub = new Validation(congress);
		congressScrub.addRule(new NonNullRule(new ObjectAccess("memberName")));
		congressScrub.addRule(new NonNullRule(TextNode.valueOf("none"), new ObjectAccess("biography")));
		 congressScrub.addRule(new PatternValidationExpression(Pattern.compile("[0-9]+\\(.*"), new ObjectAccess(
		 "congresses"), new ArrayAccess(0), new ObjectAccess("number")));
		// congressScrub
		// .addRule(new BlackListRule(Arrays.asList(TextNode.valueOf("NA")), TextNode.valueOf(""), new ObjectAccess(
		// "congress"), new ArrayProjection(new ObjectAccess("position"))));
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
		resultProjection.addMapping("biography", new PathExpression(new InputSelection(1), new ArrayAccess(0),
			new ObjectAccess("biography")));

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
