package eu.stratosphere.usecase.cleansing;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.TextNode;

import uk.ac.shef.wit.simmetrics.similaritymetrics.JaccardSimilarity;
import uk.ac.shef.wit.simmetrics.similaritymetrics.JaroWinkler;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.PersistenceType;
import eu.stratosphere.sopremo.Sink;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.aggregation.BuiltinFunctions;
import eu.stratosphere.sopremo.base.Grouping;
import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.base.UnionAll;
import eu.stratosphere.sopremo.cleansing.record_linkage.DisjunctPartitioning;
import eu.stratosphere.sopremo.cleansing.record_linkage.InterSourceRecordLinkage;
import eu.stratosphere.sopremo.cleansing.record_linkage.LinkageMode;
import eu.stratosphere.sopremo.cleansing.scrubbing.BlackListRule;
import eu.stratosphere.sopremo.cleansing.scrubbing.NonNullRule;
import eu.stratosphere.sopremo.cleansing.scrubbing.PatternValidationExpression;
import eu.stratosphere.sopremo.cleansing.scrubbing.SchemaMapping;
import eu.stratosphere.sopremo.cleansing.scrubbing.Validation;
import eu.stratosphere.sopremo.cleansing.similarity.SimmetricFunction;
import eu.stratosphere.sopremo.expressions.AggregationExpression;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.ArrayProjection;
import eu.stratosphere.sopremo.expressions.BatchAggregationExpression;
import eu.stratosphere.sopremo.expressions.CoerceExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.FunctionCall;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.expressions.TernaryExpression;

public class GovWild implements PlanAssembler, PlanAssemblerDescription {
	private int noSubTasks;

	private String inputDir;

	private String outputDir;

	private List<Operator> persons = new ArrayList<Operator>();

	private List<Operator> legalEntities = new ArrayList<Operator>();

	private List<Operator> funds = new ArrayList<Operator>();

	private List<Sink> sinks = new ArrayList<Sink>();

	private Operator personCluster, legalEntityCluster, fundCluster;

	@Override
	public Plan getPlan(String... args) throws IllegalArgumentException {
		this.noSubTasks = args.length > 0 ? Integer.parseInt(args[0]) : 1;
		this.inputDir = args.length > 1 ? args[1] : "";
		this.outputDir = args.length > 2 ? args[2] : "";

		boolean simulate = false;
		this.scrubCongress(simulate);
		this.scrubEarmark(simulate);

		this.mapCongressLegalEntity(simulate);
		this.mapCongressPerson(simulate);

		this.mapEarmarkFund(simulate);
		this.mapEarmarkPerson(simulate);
		this.mapEarmarkLegalEntity(simulate);

		clusterPersons(simulate);

//		analysis(false);

		// // Operator earmarks = new Source(PersistenceType.HDFS, String.format("%s/OriginalUsEarmark2008.json",
		// inputDir));
		// Operator congressPersons = new Source(PersistenceType.HDFS, String.format("%s/CongressPerson.json",
		// outputDir));
		// Operator earmarkPersons = new Source(PersistenceType.HDFS, String.format("%s/EarmarkPerson.json",
		// outputDir));
		// // Operator scrubbedEarmark = scrubEarmark(earmarks);
		// // Operator congress = ;
		// // Sink congressOut = cleanCongress();
		//
		// // Operator persons = clusterPersons(congressPersons, earmarkPersons);
		// // Sink earmarkFundOut = new Sink(PersistenceType.HDFS, String.format("%s/Persons.json", outputDir),
		// // persons);
		//
		// Operator persons = new Source(PersistenceType.HDFS, String.format("%s/Persons.json", outputDir));
		//
		// Operator selection = new Selection(new ComparativeExpression(new FunctionCall("count", new ArrayAccess(0)),
		// BinaryOperator.EQUAL, new ConstantExpression(0)), persons);
		//
		// Sink earmarkFundOut = new Sink(PersistenceType.HDFS, String.format("%s/Result.json", outputDir),
		// selection);
		SopremoPlan sopremoPlan = new SopremoPlan(this.sinks);
		sopremoPlan.getContext().getFunctionRegistry().register(BuiltinFunctions.class);
		sopremoPlan.getContext().getFunctionRegistry()
			.register(eu.stratosphere.usecase.cleansing.CleansFunctions.class);
		return sopremoPlan.asPactPlan();
	}

	// private Operator mapCongressLegalEntities(Operator congressScrub) {
	//
	// }
	//

	private void mapCongressPerson(boolean simulate) {
		if (simulate) {
			this.persons
				.add(new Source(PersistenceType.HDFS, String.format("%s/ScrubbedCongress.json", this.outputDir)));
			return;
		}

		ObjectCreation projection = new ObjectCreation();
		projection.addMapping("id", new GenerateExpression("congressPerson%s"));
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
		partyProjection.addMapping("legalEntity", new FunctionCall("format", new ConstantExpression(
			"congressLegalEntity%s"), new ObjectAccess("party")));
		partyProjection.addMapping("startYear",
			this.coerce(IntNode.class, new AggregationExpression(CleansFunctions.MIN, new FunctionCall(
				"extract", new ObjectAccess("number"), new ConstantExpression("\\(([0-9]{0,4})-")))));
		partyProjection.addMapping("endYear",
			this.coerce(IntNode.class, new AggregationExpression(CleansFunctions.MAX, new FunctionCall(
				"extract", new ObjectAccess("number"), new ConstantExpression("-([0-9]{0,4})")))));
		partyProjection.addMapping("congresses",
			new ArrayProjection(this.coerce(IntNode.class, new FunctionCall("extract", new ObjectAccess(
				"number"), new ConstantExpression("([0-9]+)\\(")))));
		AggExpression parties = new AggExpression(new ObjectAccess("party"), partyProjection);

		ObjectCreation stateProjections = new ObjectCreation();
		stateProjections.addMapping("legalEntity", new FunctionCall("format", new ConstantExpression(
			"congressLegalEntity%s"), new ObjectAccess("state")));
		stateProjections.addMapping("positions", new FunctionCall("distinct", new AggregationExpression(
			CleansFunctions.ALL, new ObjectAccess("position"))));
		stateProjections.addMapping("startYear",
			this.coerce(IntNode.class, new AggregationExpression(CleansFunctions.MIN, new FunctionCall(
				"extract", new ObjectAccess("number"), new ConstantExpression("\\(([0-9]{0,4})-")))));
		stateProjections.addMapping("endYear",
			this.coerce(IntNode.class, new AggregationExpression(CleansFunctions.MAX, new FunctionCall(
				"extract", new ObjectAccess("number"), new ConstantExpression("-([0-9]{0,4})")))));
		stateProjections.addMapping("congresses",
			new AggregationExpression(CleansFunctions.ALL, this.coerce(IntNode.class, new FunctionCall(
				"extract", new ObjectAccess("number"), new ConstantExpression("([0-9]+)\\(")))));
		AggExpression congresses = new AggExpression(new ObjectAccess("state"), stateProjections);

		projection.addMapping("employment", new PathExpression(new ObjectAccess("congresses"), new FunctionCall(
			"unionAll", parties, congresses)));

		if (this.scrubbedCongress == null)
			this.scrubCongress(true);
		SchemaMapping congressMapping = new SchemaMapping(projection, this.scrubbedCongress);
		this.persons.add(congressMapping);
		this.sinks.add(new Sink(PersistenceType.HDFS, String.format("%s/ScrubbedCongress.json", this.outputDir),
			congressMapping));
	}

	private Operator mapCongressStates() {
		ObjectCreation projection = new ObjectCreation();
		PathExpression name = new PathExpression(new ArrayAccess(0), new ObjectAccess("state"));
		projection.addMapping("id", new FunctionCall("format", new ConstantExpression("congressLegalEntity%s"), name));
		projection.addMapping("name", name);

		ObjectCreation address = new ObjectCreation();
		address.addMapping("state", name);
		address.addMapping("country", new ConstantExpression("United States of America"));
		projection.addMapping("addresses", address);

		Grouping grouping = new Grouping(projection, this.scrubbedCongress);
		grouping.withKeyProjection(new ObjectAccess("state"));
		return grouping;
	}

	private Operator mapCongressParties() {

		ObjectCreation projection = new ObjectCreation();
		PathExpression name = new PathExpression(new ArrayAccess(0), new ObjectAccess("party"));
		projection.addMapping("id", new FunctionCall("format", new ConstantExpression("congressLegalEntity%s"), name));

		Grouping grouping = new Grouping(projection, this.scrubbedCongress);
		grouping.withKeyProjection(new ObjectAccess("state"));
		return grouping;
	}

	private void mapCongressLegalEntity(boolean simulate) {
		if (simulate) {
			this.legalEntities.add(new Source(PersistenceType.HDFS, String.format("%s/CongressLegalEntities.json",
				this.outputDir)));
			return;
		}

		if (this.scrubbedCongress == null)
			this.scrubCongress(true);
		UnionAll legalEntities = new UnionAll(this.mapCongressParties(), this.mapCongressStates());
		this.legalEntities.add(legalEntities);
		this.sinks.add(new Sink(PersistenceType.HDFS, String.format("%s/CongressLegalEntities.json", this.outputDir),
			legalEntities));
	}

	private EvaluationExpression coerce(Class<? extends JsonNode> type, EvaluationExpression expression) {
		return new PathExpression(expression, new CoerceExpression(type));
		// return expression;
	}

	private void clusterPersons(boolean simulate) {
		if (simulate) {
			this.personCluster = new Source(PersistenceType.HDFS, String.format("%s/PersonClusters.json",
				this.outputDir));
			return;
		}

		EvaluationExpression firstName = new FunctionCall("format", new ConstantExpression("%s %s"), new ObjectAccess(
			"firstName"), new TernaryExpression(new ObjectAccess("middleName"), new ObjectAccess("middleName"),
			new ConstantExpression("")));
		FunctionCall simmFunction = new FunctionCall("average",
			new SimmetricFunction(new JaccardSimilarity(), new PathExpression(new InputSelection(0), firstName),
				new PathExpression(new InputSelection(1), firstName)),
			new SimmetricFunction(new JaroWinkler(), new PathExpression(new InputSelection(0), new ObjectAccess(
				"lastName")), new PathExpression(new InputSelection(1), new ObjectAccess("lastName"))));
		// DisjunctPartitioning partitioning = new DisjunctPartitioning(new FunctionCall("substring", new
		// ObjectAccess("lastName"), new ConstantExpression(0), new ConstantExpression(2)));
		DisjunctPartitioning partitioning = new DisjunctPartitioning(new ObjectAccess("lastName"));
		InterSourceRecordLinkage recordLinkage = new InterSourceRecordLinkage(partitioning, simmFunction, 0.6,
			this.persons);
		recordLinkage.setLinkageMode(LinkageMode.ALL_CLUSTERS_PROVENANCE);

		ObjectCreation projection = new ObjectCreation();
		projection.addMapping("firstName", new ObjectAccess("firstName"));
		projection.addMapping("middleName", new ObjectAccess("middleName"));
		projection.addMapping("state", new PathExpression(new ObjectAccess("employment"), new ArrayAccess(-1),
			new ObjectAccess("legalEntity")));
		// recordLinkage.getRecordLinkageInput(0).setResultProjection(projection);
		// recordLinkage.getRecordLinkageInput(1).setResultProjection(projection);

		// recordLinkage.getRecordLinkageInput(0).setIdProjection(new ObjectAccess("id"));
		recordLinkage.getRecordLinkageInput(1).setIdProjection(projection);

		this.personCluster = recordLinkage;
		this.sinks.add(new Sink(PersistenceType.HDFS, String.format("%s/PersonClusters.json", this.outputDir),
			recordLinkage));
	}

	private void mapEarmarkFund(boolean simulate) {
		if (simulate) {
			this.funds.add( new Source(PersistenceType.HDFS, String.format("%s/EarmarkFunds.json",
				this.outputDir)));
			return;
		}

		ObjectCreation fundProjection = new ObjectCreation();

		final BatchAggregationExpression batch = new BatchAggregationExpression();
		fundProjection.addMapping("id", new FunctionCall("format", new ConstantExpression("earmark%d"),
			new PathExpression(batch.add(CleansFunctions.FIRST), new ObjectAccess("earmarkId"))));
		fundProjection.addMapping("amount", batch.add(CleansFunctions.SUM, new TernaryExpression(new ObjectAccess(
			"amount"), new ObjectAccess("amount"), new ConstantExpression(0))));
		fundProjection.addMapping("currency", new ConstantExpression("dollar"));
		ObjectCreation date = new ObjectCreation();
		date.addMapping("year", batch.add(CleansFunctions.FIRST, new ObjectAccess("enactedYear")));
		fundProjection.addMapping("date", date);
		fundProjection.addMapping("subject", new PathExpression(batch.add(CleansFunctions.FIRST), new ObjectAccess(
			"shortDescription")));

		if (this.scrubbedEarmarks == null)
			this.scrubEarmark(true);
		Grouping grouping = new Grouping(fundProjection, this.scrubbedEarmarks);
		grouping.withKeyProjection(new ObjectAccess("earmarkId"));

		this.funds.add(grouping);
		this.sinks.add(new Sink(PersistenceType.HDFS, String.format("%s/EarmarkFunds.json", this.outputDir), grouping));
	}

	private Operator mapEarmarkStates() {
		ObjectCreation projection = new ObjectCreation();
		PathExpression name = new PathExpression(new ArrayAccess(0), new ObjectAccess("sponsorStateCode"));
		projection.addMapping("id", new FunctionCall("format", new ConstantExpression("congressLegalEntity%s"), name));
		projection.addMapping("name", name);

		ObjectCreation address = new ObjectCreation();
		address.addMapping("state", name);
		address.addMapping("country", new ConstantExpression("United States of America"));
		projection.addMapping("addresses", address);

		ComparativeExpression recipientCondition = new ComparativeExpression(new ObjectAccess("sponsorStateCode"),
			BinaryOperator.NOT_EQUAL, new ConstantExpression(""));
		Selection sponsors = new Selection(recipientCondition, this.scrubbedEarmarks);
		Grouping grouping = new Grouping(projection, sponsors);
		grouping.withKeyProjection(new ObjectAccess("sponsorStateCode"));
		return grouping;
	}

	private void mapEarmarkLegalEntity(boolean simulate) {
		if (simulate) {
			this.legalEntities.add( new Source(PersistenceType.HDFS, String.format("%s/ScrubbedCongress.json",
				this.outputDir)));
			return;
		}

		if (this.scrubbedEarmarks == null)
			this.scrubEarmark(true);
		UnionAll legalEntities = new UnionAll(this.mapEarmarkReceiver(), this.mapEarmarkStates());
		this.legalEntities.add(legalEntities);
		this.sinks.add(new Sink(PersistenceType.HDFS, String.format("%s/EarmarkLegalEntities.json", this.outputDir),
			legalEntities));

	}

	private Operator mapEarmarkReceiver() {

		ObjectCreation projection = new ObjectCreation();

		final BatchAggregationExpression batch = new BatchAggregationExpression();

		projection.addMapping("name", new PathExpression(batch.add(CleansFunctions.FIRST),
			new ObjectAccess("recipient")));

		ObjectCreation funds = new ObjectCreation();
		funds.addMapping("id", new GenerateExpression("earmarkReceiver%s"));
		funds.addMapping("fund", new FunctionCall("format", new ConstantExpression("earmark%s"), new ObjectAccess(
			"earmarkId")));
		funds.addMapping("amount", new ObjectAccess("amount"));
		funds.addMapping("currency", new ConstantExpression("dollar"));
		projection.addMapping("receivedFunds", batch.add(CleansFunctions.ALL, funds));

		ObjectCreation address = new ObjectCreation();
		address.addMapping("street", new FunctionCall("trim",
			new FunctionCall("format", new ConstantExpression("%s %s"), new ObjectAccess("recipientStreet1"),
				new ObjectAccess("recipientStreet2"))));
		address.addMapping("city", new ObjectAccess("recipientCity"));
		address.addMapping("zipCode", new ObjectAccess("recipientZipcode"));
		address.addMapping("state", new FunctionCall("camelCase", new ObjectAccess("sponsorStateCode")));
		address.addMapping("country", new ObjectAccess("recipientCountry"));
		projection.addMapping("addresses", new FunctionCall("distinct", batch.add(CleansFunctions.ALL, address)));

		projection.addMapping("phones",
			new FunctionCall("distinct", batch.add(CleansFunctions.ALL, new ObjectAccess("phone"))));
		// TODO: add recipientType, recipientTypeOther

		ComparativeExpression recipientCondition = new ComparativeExpression(new ObjectAccess("recordType"),
			BinaryOperator.EQUAL, new ConstantExpression("R"));
		Selection sponsors = new Selection(recipientCondition, this.scrubbedEarmarks);
		Grouping grouping = new Grouping(projection, sponsors);
		grouping.withKeyProjection(new ObjectAccess("recipient"));

		return grouping;
	}

	private void mapEarmarkPerson(boolean simulate) {
		if (simulate) {
			this.persons.add( new Source(PersistenceType.HDFS, String.format("%s/EarmarkPersons.json",
				this.outputDir)));
			return;
		}

		ObjectCreation projection = new ObjectCreation();

		final BatchAggregationExpression batch = new BatchAggregationExpression();

		projection.addMapping("id", new GenerateExpression("earmarkPerson%s"));
		projection.addMapping("firstName", new PathExpression(batch.add(CleansFunctions.FIRST), new TernaryExpression(
			new ObjectAccess("sponsorLastName"), new ObjectAccess("sponsorFirstName"), new ConstantExpression(""))));
		projection.addMapping("middleName", new PathExpression(batch.add(CleansFunctions.FIRST), new ObjectAccess(
			"sponsorMiddleName"), new TernaryExpression(EvaluationExpression.VALUE, EvaluationExpression.VALUE)));
		projection.addMapping("lastName", new PathExpression(batch.add(CleansFunctions.FIRST), new TernaryExpression(
			new ObjectAccess("sponsorLastName"), new ObjectAccess("sponsorLastName"), new ObjectAccess(
				"sponsorFirstName"))));

		projection.addMapping("enactedFunds", batch.add(CleansFunctions.SORT, new FunctionCall("format",
			new ConstantExpression("earmark%s"), new ObjectAccess("earmarkId"))));

		ObjectCreation address = new ObjectCreation();
		address.addMapping("state", new ObjectAccess("sponsorStateCode"));
		projection.addMapping("addresses", new FunctionCall("distinct", batch.add(CleansFunctions.ALL, address)));

		ObjectCreation employment = new ObjectCreation();
		employment.addMapping("legalEntity", new FunctionCall("format",
			new ConstantExpression("congressLegalEntity%s"), new ObjectAccess("sponsorStateCode")));
		employment.addMapping("startYear", new ConstantExpression(2008));
		employment.addMapping("endYear", new ConstantExpression(2008));
		projection.addMapping("position", new ObjectAccess("sponsorHonorific"));

		if (this.scrubbedEarmarks == null)
			this.scrubEarmark(true);
		ComparativeExpression sponsorCondition = new ComparativeExpression(new ObjectAccess("recordType"),
			BinaryOperator.EQUAL, new ConstantExpression("C"));
		Selection sponsors = new Selection(sponsorCondition, this.scrubbedEarmarks);
		Grouping grouping = new Grouping(projection, sponsors);
		grouping.withKeyProjection(new ArrayCreation(new ObjectAccess("sponsorFirstName"),
			new ObjectAccess("sponsorMiddleName"), new ObjectAccess("sponsorLastName")));

		this.persons.add(grouping);
		this.sinks
			.add(new Sink(PersistenceType.HDFS, String.format("%s/EarmarkPerson.json", this.outputDir), grouping));

	}

	private void scrubEarmark(boolean simulate) {
		if (simulate) {
			this.scrubbedEarmarks = new Source(PersistenceType.HDFS, String.format("%s/ScrubbedEarmarks.json",
				this.outputDir));
			return;
		}

		Source earmarks = new Source(PersistenceType.HDFS,
			String.format("%s/OriginalUsEarmark2008.json", this.inputDir));
		Validation validation = new Validation(earmarks);

		// validation.addRule(new RangeRule(new ObjectAccess("memberName")));
		this.scrubbedEarmarks = validation;
		this.sinks.add(new Sink(PersistenceType.HDFS, String.format("%s/ScrubbedEarmarks.json", this.outputDir), validation));
	}

	private Operator scrubbedCongress, scrubbedEarmarks;

	private void scrubCongress(boolean simulate) {
		if (simulate) {
			this.scrubbedCongress = new Source(PersistenceType.HDFS, String.format("%s/ScrubbedCongress.json",
				this.outputDir));
			return;
		}

		Validation scrubbedCongress = new Validation(this.joinCongres());
		scrubbedCongress.addRule(new NonNullRule(new ObjectAccess("memberName")));
		scrubbedCongress.addRule(new NonNullRule(TextNode.valueOf("none"), new ObjectAccess("biography")));
		scrubbedCongress.addRule(new PatternValidationExpression(Pattern.compile("[0-9]+\\(.*"), new ObjectAccess(
			"congresses"), new ArrayAccess(-1), new ObjectAccess("number")));
		scrubbedCongress
			.addRule(new BlackListRule(Arrays.asList(TextNode.valueOf("NA")), TextNode.valueOf(""), new ObjectAccess(
				"congress"), new ArrayProjection(new ObjectAccess("position"))));
		this.scrubbedCongress = scrubbedCongress;

		this.sinks.add(new Sink(PersistenceType.HDFS, String.format("%s/ScrubbedCongress.json", this.outputDir),
			scrubbedCongress));
	}

	private Operator joinCongres() {

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
			new Source(PersistenceType.HDFS, String.format("%s/OriginalUsCongress.json", this.inputDir)),
			new Source(PersistenceType.HDFS, String.format("%s/OriginalUsCongressBiography.json", this.inputDir)));

		congress.withKeyProjection(0, new ObjectAccess("biography"));
		congress.withKeyProjection(1, new ObjectAccess("id"));

		return congress;
	}

	public static void main(String[] args) {
		// new GovWild().getPlan(args);

		File dir = new File("/home/arv/workflow/input");
		for (File file : dir.listFiles())
			if (file.getName().contains("Free"))
				try {
					final JsonParser parser = JsonUtil.FACTORY.createJsonParser(file);
					parser.setCodec(JsonUtil.OBJECT_MAPPER);
					parser.readValueAsTree();
				} catch (Exception e) {
					System.out.println(file + " not parsed" + e);
				}
	}

	@Override
	public String getDescription() {
		return "Parameters: [noSubStasks] [inputDir] [outputDir]";
	}
}
