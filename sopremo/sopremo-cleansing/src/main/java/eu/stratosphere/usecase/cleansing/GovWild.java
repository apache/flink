package eu.stratosphere.usecase.cleansing;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.DecimalNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.TextNode;

import uk.ac.shef.wit.simmetrics.similaritymetrics.JaccardSimilarity;
import uk.ac.shef.wit.simmetrics.similaritymetrics.JaroWinkler;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.io.OutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.util.ReflectionUtil;
import eu.stratosphere.pact.testing.ioformats.SequentialInputFormat;
import eu.stratosphere.pact.testing.ioformats.SequentialOutputFormat;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.PersistenceType;
import eu.stratosphere.sopremo.Sink;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.aggregation.BuiltinFunctions;
import eu.stratosphere.sopremo.base.Grouping;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.base.UnionAll;
import eu.stratosphere.sopremo.cleansing.record_linkage.DisjunctPartitioning;
import eu.stratosphere.sopremo.cleansing.record_linkage.InterSourceRecordLinkage;
import eu.stratosphere.sopremo.cleansing.record_linkage.LinkageMode;
import eu.stratosphere.sopremo.cleansing.record_linkage.Naive;
import eu.stratosphere.sopremo.cleansing.record_linkage.ValueSplitter;
import eu.stratosphere.sopremo.cleansing.scrubbing.BlackListRule;
import eu.stratosphere.sopremo.cleansing.scrubbing.NonNullRule;
import eu.stratosphere.sopremo.cleansing.scrubbing.PatternValidationExpression;
import eu.stratosphere.sopremo.cleansing.scrubbing.SchemaMapping;
import eu.stratosphere.sopremo.cleansing.scrubbing.Validation;
import eu.stratosphere.sopremo.cleansing.similarity.MongeElkanSimilarity;
import eu.stratosphere.sopremo.cleansing.similarity.SimmetricFunction;
import eu.stratosphere.sopremo.expressions.AggregationExpression;
import eu.stratosphere.sopremo.expressions.AndExpression;
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
import eu.stratosphere.sopremo.expressions.UnaryExpression;
import eu.stratosphere.sopremo.pact.CsvInputFormat;
import eu.stratosphere.sopremo.pact.JsonInputFormat;
import eu.stratosphere.sopremo.pact.JsonOutputFormat;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.PactJsonObject.Key;
import eu.stratosphere.util.reflect.ReflectUtil;

public class GovWild implements PlanAssembler, PlanAssemblerDescription {
	private int noSubTasks;

	private String inputDir;

	private String outputDir;

	private List<Sink> sinks = new ArrayList<Sink>();

	private static final int CONGRESS = 0, EARMARK = 1, SPENDING = 2, FREEBASE = 3;

	private static final int PERSON = 0, LEGAL_ENTITY = 1, FUND = 2;

	private Operator[][] inputs = new Operator[4][3];

	// cluster[x][y] after integration of source x
	private Operator[][] cluster = new Operator[4][3];

	private Operator[][] fused = new Operator[4][3];

	@Override
	public Plan getPlan(String... args) throws IllegalArgumentException {
		this.noSubTasks = args.length > 0 ? Integer.parseInt(args[0]) : 1;
		this.inputDir = args.length > 1 ? args[1] : "";
		this.outputDir = args.length > 2 ? args[2] : "";

		int startLevel = args.length > 3 ? Integer.parseInt(args[3]) : 0, stopLevel = args.length > 4 ? Integer
			.parseInt(args[4]) : Integer.MAX_VALUE;

		 this.joinCongress(!(startLevel <= 0 && 0 < stopLevel));
		
		 this.scrubCongress(!(startLevel <= 1 && 1 < stopLevel));
		 this.scrubEarmark(!(startLevel <= 1 && 1 < stopLevel));
		
		this.mapCongressLegalEntity(!(startLevel <= 2 && 2 < stopLevel));
		 this.mapCongressPerson(!(startLevel <= 2 && 2 < stopLevel));
		
		 this.mapEarmarkFund(!(startLevel <= 3 && 3 < stopLevel));
		 this.mapEarmarkPerson(!(startLevel <= 3 && 3 < stopLevel));
		this.mapEarmarkLegalEntity(!(startLevel <= 3 && 3 < stopLevel));
		 //
		 clusterPersons1(!(startLevel <= 10 && 10 < stopLevel));
		 fusePersons1(!(startLevel <= 11 && 11 < stopLevel));
		clusterLegalEntity1(!(startLevel <= 12 && 12 < stopLevel));
		fuseLegalEntity1(!(startLevel <= 13 && 13 < stopLevel));

		this.scrubSpending(!(startLevel <= 21 && 21 < stopLevel));
		this.mapSpendingFund(!(startLevel <= 22 && 22 < stopLevel));
		this.mapSpendingLegalEntity(!(startLevel <= 22 && 22 < stopLevel));

		clusterLegalEntity2(!(startLevel <= 31 && 31 < stopLevel));
		fuseLegalEntity2(!(startLevel <= 32 && 32 < stopLevel));
		
		if(stopLevel == 1337) {
			sinks.clear();
			sinks.add((Sink) this.fused[SPENDING][LEGAL_ENTITY]);
			sinks.add((Sink) this.fused[EARMARK][PERSON]);
			sinks.add((Sink) this.inputs[SPENDING][FUND]);
			sinks.add((Sink) this.inputs[EARMARK][FUND]);
		}
		// //
//		 analyze(!(startLevel <= 100 && 100 < stopLevel));

		// // Operator earmarks = new Source(getInternalInputFormat(), String.format("%s/OriginalUsEarmark2008.json",
		// inputDir));
		// Operator congressPersons = new Source(getInternalInputFormat(), String.format("%s/CongressPerson.json",
		// outputDir));
		// Operator earmarkPersons = new Source(getInternalInputFormat(), String.format("%s/EarmarkPerson.json",
		// outputDir));
		// // Operator scrubbedEarmark = scrubEarmark(earmarks);
		// // Operator congress = ;
		// // Sink congressOut = cleanCongress();
		//
		// // Operator persons = clusterPersons(congressPersons, earmarkPersons);
		// // Sink earmarkFundOut = new Sink(SequentialOutputFormat.class, String.format("%s/Persons.json", outputDir),
		// // persons);
		//
		// Operator persons = new Source(getInternalInputFormat(), String.format("%s/Persons.json", outputDir));
		//
		// Operator selection = new Selection(new ComparativeExpression(new FunctionCall("count", new ArrayAccess(0)),
		// BinaryOperator.EQUAL, new ConstantExpression(0)), persons);
		//
		// Sink earmarkFundOut = new Sink(SequentialOutputFormat.class, String.format("%s/Result.json", outputDir),
		// selection);
		SopremoPlan sopremoPlan = new SopremoPlan(this.sinks);
		sopremoPlan.getContext().getFunctionRegistry().register(BuiltinFunctions.class);
		sopremoPlan.getContext().getFunctionRegistry()
			.register(eu.stratosphere.usecase.cleansing.CleansFunctions.class);
		Plan pactPlan = sopremoPlan.asPactPlan();
		pactPlan.setMaxNumberMachines(noSubTasks);
		return pactPlan;
	}

	private void analyze(boolean simulate) {

		Operator persons = fused[EARMARK][PERSON];
		if (persons == null)
			persons = new Source(String.format("%s/Persons.json", outputDir));

		Operator selection = new Selection(new ComparativeExpression(new FunctionCall("count", new ArrayAccess(0)),
			BinaryOperator.EQUAL, new ConstantExpression(0)), persons);

		sinks.add(new Sink(getInternalOutputFormat(), String.format("%s/Result.json", outputDir), selection));

	}

	private void clusterLegalEntity1(boolean simulate) {
		if (simulate) {
			this.cluster[EARMARK][LEGAL_ENTITY] = new Source(getInternalInputFormat(), String.format(
				"%s/LegalEntityCluster1.json",				this.outputDir));
			return;
		}

		SimmetricFunction simmFunction = new SimmetricFunction(new JaroWinkler(), 
				new PathExpression(new InputSelection(0), new ObjectAccess("names"), new ArrayAccess(0)),
				new PathExpression(new InputSelection(1), new ObjectAccess("names"), new ArrayAccess(0)));
		// DisjunctPartitioning partitioning = new DisjunctPartitioning(new FunctionCall("substring", new
		// ObjectAccess("lastName"), new ConstantExpression(0), new ConstantExpression(2)));
		InterSourceRecordLinkage recordLinkage = new InterSourceRecordLinkage(new Naive(), simmFunction, 0.8,
			this.inputs[CONGRESS][LEGAL_ENTITY], this.inputs[EARMARK][LEGAL_ENTITY]);
		recordLinkage.setLinkageMode(LinkageMode.ALL_CLUSTERS_FLAT);

		recordLinkage.getRecordLinkageInput(0).setIdProjection(new ObjectAccess("id"));
		recordLinkage.getRecordLinkageInput(1).setIdProjection(new ObjectAccess("id"));

		this.cluster[EARMARK][LEGAL_ENTITY] = recordLinkage;
		this.sinks.add(new Sink(getInternalOutputFormat(), String.format("%s/LegalEntityCluster1.json", this.outputDir),
			recordLinkage));
	}

	private void clusterPersons1(boolean simulate) {
		if (simulate) {
			this.cluster[EARMARK][PERSON] = new Source(getInternalInputFormat(), String.format("%s/PersonCluster1.json",
				this.outputDir));
			return;
		}

		EvaluationExpression firstName = new FunctionCall("format", new ObjectAccess("firstName"));
		FunctionCall simmFunction = new FunctionCall("average",
			new SimmetricFunction(new JaccardSimilarity(), new PathExpression(new InputSelection(0), firstName),
				new PathExpression(new InputSelection(1), firstName)),
			new SimmetricFunction(new JaroWinkler(), new PathExpression(new InputSelection(0), new ObjectAccess(
				"lastName")), new PathExpression(new InputSelection(1), new ObjectAccess("lastName"))));
		// DisjunctPartitioning partitioning = new DisjunctPartitioning(new FunctionCall("substring", new
		// ObjectAccess("lastName"), new ConstantExpression(0), new ConstantExpression(2)));
		DisjunctPartitioning partitioning = new DisjunctPartitioning(new ObjectAccess("lastName"));
		InterSourceRecordLinkage recordLinkage = new InterSourceRecordLinkage(partitioning, simmFunction, 0.6,
			this.inputs[CONGRESS][PERSON], this.inputs[EARMARK][PERSON]);
		recordLinkage.setLinkageMode(LinkageMode.ALL_CLUSTERS_FLAT);

		recordLinkage.getRecordLinkageInput(0).setIdProjection(new ObjectAccess("id"));
		recordLinkage.getRecordLinkageInput(1).setIdProjection(new ObjectAccess("id"));

		this.cluster[EARMARK][PERSON] = recordLinkage;
		this.sinks.add(new Sink(getInternalOutputFormat(), String.format("%s/PersonCluster1.json", this.outputDir),
			recordLinkage));
	}

	private void clusterLegalEntity2(boolean simulate) {
		if (simulate) {
			this.cluster[SPENDING][LEGAL_ENTITY] = new Source(getInternalInputFormat(), String.format(
				"%s/LegalEntityCluster2.json",
				this.outputDir));
			return;
		}

//		SimmetricFunction baseMeasure = new SimmetricFunction(new JaroWinkler(), new InputSelection(0),
//			new InputSelection(1));
//		EvaluationExpression simmFunction = new MongeElkanSimilarity(baseMeasure,
//				new PathExpression(new InputSelection(0), new ObjectAccess("names")),
//				new PathExpression(new InputSelection(1), new ObjectAccess("names")));

		SimmetricFunction simmFunction = new SimmetricFunction(new JaroWinkler(), 
				new PathExpression(new InputSelection(0), new ObjectAccess("names"), new ArrayAccess(0)),
				new PathExpression(new InputSelection(1), new ObjectAccess("names"), new ArrayAccess(0)));
		// DisjunctPartitioning partitioning = new DisjunctPartitioning(new FunctionCall("substring", new
		// ObjectAccess("lastName"), new ConstantExpression(0), new ConstantExpression(2)));
		DisjunctPartitioning partitioning = new DisjunctPartitioning(
			new TernaryExpression(
				new AndExpression(new ObjectAccess("addresses"), 
					new ComparativeExpression(new ConstantExpression(2), BinaryOperator.LESS_EQUAL, 
						new FunctionCall("length", new PathExpression(new ObjectAccess("addresses"),
							new ArrayAccess(0), new ObjectAccess("zipCode"))))),
				new FunctionCall("substring", new PathExpression(new ObjectAccess("addresses"), new ArrayAccess(0),
					new ObjectAccess("zipCode")), new ConstantExpression(0), new ConstantExpression(2)),
				new ConstantExpression("")),
				new FunctionCall("extract", new ConstantExpression("([^ ])*"), 
					new PathExpression(new ObjectAccess("names"), new ArrayAccess(0))));
		InterSourceRecordLinkage recordLinkage = new InterSourceRecordLinkage(partitioning, simmFunction, 0.8,
			this.fused[EARMARK][LEGAL_ENTITY], this.inputs[SPENDING][LEGAL_ENTITY]);
		recordLinkage.setLinkageMode(LinkageMode.ALL_CLUSTERS_FLAT);

		recordLinkage.getRecordLinkageInput(0).setIdProjection(new ObjectAccess("id"));
		recordLinkage.getRecordLinkageInput(1).setIdProjection(new ObjectAccess("id"));

		this.cluster[SPENDING][LEGAL_ENTITY] = recordLinkage;
		this.sinks.add(new Sink(getInternalOutputFormat(), String.format("%s/LegalEntityCluster2.json", this.outputDir),
			recordLinkage));
	}

	private void fusePersons1(boolean simulate) {
		if (simulate) {
			this.fused[EARMARK][PERSON] = new Source(getInternalInputFormat(), String.format("%s/Persons1.json",
				this.outputDir));
			return;
		}

		Selection links = new Selection(new ComparativeExpression(
			new FunctionCall("count", EvaluationExpression.VALUE), BinaryOperator.GREATER, new ConstantExpression(1)),
			this.cluster[EARMARK][PERSON]);

		Selection earmarksOnly = new Selection(new ComparativeExpression(
			new FunctionCall("count", EvaluationExpression.VALUE), BinaryOperator.EQUAL, new ConstantExpression(1)),
			this.cluster[EARMARK][PERSON]);

		ObjectCreation merge = new ObjectCreation();
		merge.addMapping(new ObjectCreation.CopyFields(new InputSelection(0)));
		merge.addMapping("funds", new PathExpression(new InputSelection(1), new ObjectAccess("enactedFunds")));
		Projection linksProjection = new Projection(merge, links);

		Projection earmarkProjection = new Projection(new InputSelection(0), earmarksOnly);

		this.fused[EARMARK][PERSON] = new UnionAll(linksProjection, earmarkProjection);
		this.sinks.add(new Sink(getInternalOutputFormat(), String.format("%s/Person1.json", this.outputDir),
			this.fused[EARMARK][PERSON]));
	}

	private void fuseLegalEntity1(boolean simulate) {
		if (simulate) {
			this.fused[EARMARK][LEGAL_ENTITY] = new Source(getInternalInputFormat(), String.format("%s/LegalEntity1.json",
				this.outputDir));
			return;
		}

		Selection links = new Selection(new ComparativeExpression(
			new FunctionCall("count", EvaluationExpression.VALUE), BinaryOperator.GREATER, new ConstantExpression(1)),
			this.cluster[EARMARK][LEGAL_ENTITY]);

		Selection earmarksOnly = new Selection(new ComparativeExpression(
			new FunctionCall("count", EvaluationExpression.VALUE), BinaryOperator.EQUAL, new ConstantExpression(1)),
			this.cluster[EARMARK][LEGAL_ENTITY]);

		ObjectCreation merge = new ObjectCreation();
		merge.addMapping(new ObjectCreation.CopyFields(new InputSelection(1)));
		merge.addMapping("names", new FunctionCall("distinct", new FunctionCall("unionAll",
			new PathExpression(new InputSelection(0), new ObjectAccess("names")),
			new PathExpression(new InputSelection(1), new ObjectAccess("names")))));
		merge.addMapping("addresses", new FunctionCall("distinct", new FunctionCall("unionAll",
			new PathExpression(new InputSelection(0), new ObjectAccess("addresses")),
			new PathExpression(new InputSelection(1), new ObjectAccess("addresses")))));
		// merge.addMapping("funds", new PathExpression(new ArrayAccess(1), new ObjectAccess("enactedFunds")));
		Projection linksProjection = new Projection(merge, links);

		Projection earmarkProjection = new Projection(new InputSelection(0), earmarksOnly);

		// TODO: update person worksFor

		this.fused[EARMARK][LEGAL_ENTITY] = new UnionAll(linksProjection, earmarkProjection);
		this.sinks.add(new Sink(getInternalOutputFormat(), String.format("%s/LegalEntity1.json", this.outputDir),
			this.fused[EARMARK][LEGAL_ENTITY]));
	}

	private void fuseLegalEntity2(boolean simulate) {
		if (simulate) {
			this.fused[SPENDING][LEGAL_ENTITY] = new Source(getInternalInputFormat(), String.format("%s/LegalEntity2.json",
				this.outputDir));
			return;
		}

		Selection links = new Selection(new ComparativeExpression(
			new FunctionCall("count", EvaluationExpression.VALUE), BinaryOperator.GREATER, new ConstantExpression(1)),
			this.cluster[EARMARK][LEGAL_ENTITY]);

		Selection oneSourceOnly = new Selection(new ComparativeExpression(
			new FunctionCall("count", EvaluationExpression.VALUE), BinaryOperator.EQUAL, new ConstantExpression(1)),
			this.cluster[EARMARK][LEGAL_ENTITY]);
		
		ObjectCreation merge = new ObjectCreation();
		merge.addMapping(new ObjectCreation.CopyFields(new InputSelection(1)));
		merge.addMapping("names", new FunctionCall("distinct", new FunctionCall("unionAll",
			new PathExpression(new InputSelection(0), new ObjectAccess("names")),
			new PathExpression(new InputSelection(1), new ObjectAccess("names")))));
		merge.addMapping("addresses", new FunctionCall("distinct", new FunctionCall("unionAll",
			new PathExpression(new InputSelection(0), new ObjectAccess("addresses")),
			new PathExpression(new InputSelection(1), new ObjectAccess("addresses")))));
		// merge.addMapping("funds", new PathExpression(new ArrayAccess(1), new ObjectAccess("enactedFunds")));
		Projection linksProjection = new Projection(merge, links);

		Projection oneSourceProjection = new Projection(new ArrayAccess(0), oneSourceOnly);

		// TODO: update person worksFor

		this.fused[SPENDING][LEGAL_ENTITY] = new UnionAll(linksProjection, oneSourceProjection);
		this.sinks.add(new Sink(getInternalOutputFormat(), String.format("%s/LegalEntity2.json", this.outputDir),
			this.fused[SPENDING][LEGAL_ENTITY]));
	}

	// private Operator mapCongressLegalEntities(Operator congressScrub) {
	//
	// }
	//

	private void mapCongressPerson(boolean simulate) {
		if (simulate) {
			this.inputs[CONGRESS][PERSON] = new Source(String.format("%s/CongressPersons.json", this.outputDir));
			return;
		}

		ObjectCreation projection = new ObjectCreation();
		projection.addMapping("id", new GenerateExpression("congressPerson%s"));
		projection.addMapping("firstName",
			new FunctionCall("extract", new ObjectAccess("memberName"), new ConstantExpression(", (.+)")));
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

		// TODO: filter party = " " and congress = 0
		ObjectCreation partyProjection = new ObjectCreation();
		partyProjection.addMapping("legalEntity", new FunctionCall("format", new ConstantExpression(
			"congressLegalEntity%s"), new PathExpression(new ArrayAccess(0), new ObjectAccess("party"))));
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
			"congressLegalEntity%s"), new PathExpression(new ArrayAccess(0), new ObjectAccess("state"))));
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

		ObjectCreation relative = new ObjectCreation();
		relative.addMapping("id", new FunctionCall("extract", EvaluationExpression.VALUE, new ConstantExpression(
			"of (.*)")));
		relative.addMapping("type", new FunctionCall("extract", EvaluationExpression.VALUE, new ConstantExpression(
			"(.*) of")));
		relative.addMapping("complete", EvaluationExpression.VALUE);
		// // projection.addMapping("relatives",
		// // new FunctionCall("split",
		// // new FunctionCall("extract", new ObjectAccess("biography"), new
		// ConstantExpression("^[^.]+,[^.]+, \\((.*?)\\)*"), new ConstantExpression("")),
		// // new ConstantExpression(", ")));
		// projection.addMapping("relatives",
		// new PathExpression(
		// new FunctionCall("extract", new ObjectAccess("biography"), new
		// ConstantExpression(".*?,.*?, (?:\\((.*?)\\))*"), new ConstantExpression("")) ));
		FunctionCall relativeExtraction = new FunctionCall("extract", new ObjectAccess("biography"),
			new ConstantExpression("^[^,]+,[^,]+, +\\((.*?)\\)"), new ConstantExpression(""));
		FunctionCall relativeWithoutAnd = new FunctionCall("replace", relativeExtraction, new ConstantExpression(
			"(?:,|;)? and "), new ConstantExpression(", "));
		FunctionCall commaSplit = new FunctionCall("split", relativeWithoutAnd, new ConstantExpression("(?:,|;) "));
		EvaluationExpression trimmedSplit = new PathExpression(commaSplit, new ArrayProjection(new FunctionCall(
			"trim", EvaluationExpression.VALUE)));
		projection.addMapping("relatives",
			new PathExpression(
				new FunctionCall("filter", trimmedSplit, new ConstantExpression("")),
				new ArrayProjection(relative)));
		// //
		if (this.scrubbed[CONGRESS] == null)
			this.scrubCongress(true);
		SchemaMapping congressMapping = new SchemaMapping(projection, this.scrubbed[CONGRESS]);

		// new Lookup(congressMapping, congressMapping);
		// ValueSplitter titles = new ValueSplitter(congressMapping).withArrayProjection(new
		// ObjectAccess("relatives"));// .withValueProjection(new
		// ArrayAccess(0))));
		// this.sinks.add(new Sink(SequentialOutputFormat.class, String.format("%s/Titles.json", this.outputDir), titles));

		this.inputs[CONGRESS][PERSON] = congressMapping;
		this.sinks.add(new Sink(getInternalOutputFormat(), String.format("%s/CongressPersons.json", this.outputDir),
			congressMapping));
	}

	private Operator mapCongressStates() {
		ObjectCreation projection = new ObjectCreation();
		BatchAggregationExpression batch = new BatchAggregationExpression();
		EvaluationExpression first = batch.add(BuiltinFunctions.FIRST);
		projection.addMapping("id", new FunctionCall("format", new ConstantExpression("congressLegalEntity%s"), first));
		projection.addMapping("names", new ArrayCreation(first));

		ObjectCreation address = new ObjectCreation();
		address.addMapping("state", first);
		address.addMapping("country", new ConstantExpression("United States of America"));
		projection.addMapping("addresses", address);

		ValueSplitter valueSplitter = new ValueSplitter(this.scrubbed[CONGRESS]).
			withArrayProjection(new ObjectAccess("congresses")).
			withKeyProjection(new PathExpression(new ArrayAccess(0), new ObjectAccess("state"))).
			withValueProjection(new PathExpression(new ArrayAccess(0), new ObjectAccess("state")));
		Grouping grouping = new Grouping(projection, valueSplitter);
		grouping.withKeyProjection(EvaluationExpression.KEY);
		return grouping;
	}

	private Operator mapCongressParties() {

		ObjectCreation projection = new ObjectCreation();
		BatchAggregationExpression batch = new BatchAggregationExpression();
		EvaluationExpression first = batch.add(BuiltinFunctions.FIRST);
		projection.addMapping("id", new FunctionCall("format", new ConstantExpression("congressLegalEntity%s"), first));
		projection.addMapping("names", new ArrayCreation(first));

		ValueSplitter valueSplitter = new ValueSplitter(this.scrubbed[CONGRESS]).
			withArrayProjection(new ObjectAccess("congresses")).
			withKeyProjection(new PathExpression(new ArrayAccess(0), new ObjectAccess("party"))).
			withValueProjection(new PathExpression(new ArrayAccess(0), new ObjectAccess("party")));
		Grouping grouping = new Grouping(projection, valueSplitter);
		grouping.withKeyProjection(EvaluationExpression.KEY);
		return grouping;
	}

	private void mapCongressLegalEntity(boolean simulate) {
		if (simulate) {
			this.inputs[CONGRESS][LEGAL_ENTITY] = new Source(getInternalInputFormat(), String.format(
				"%s/CongressLegalEntities.json",
				this.outputDir));
			return;
		}

		if (this.scrubbed[CONGRESS] == null)
			this.scrubCongress(true);
		UnionAll legalEntities = new UnionAll(this.mapCongressParties(), this.mapCongressStates());
		this.inputs[CONGRESS][LEGAL_ENTITY] = legalEntities;
		this.sinks.add(new Sink(getInternalOutputFormat(), String.format("%s/CongressLegalEntities.json", this.outputDir),
			legalEntities));
	}

	private EvaluationExpression coerce(Class<? extends JsonNode> type, EvaluationExpression expression) {
		return new PathExpression(expression, new TernaryExpression(EvaluationExpression.VALUE, new CoerceExpression(
			type)));
		// return expression;
	}

	private void mapSpendingFund(boolean simulate) {
		if (simulate) {
			this.inputs[SPENDING][FUND] = new Source(getInternalInputFormat(), String.format("%s/EarmarkFunds.json",
				this.outputDir));
			return;
		}

		ObjectCreation fundProjection = new ObjectCreation();

		final BatchAggregationExpression batch = new BatchAggregationExpression();
		fundProjection.addMapping("id", new FunctionCall("format", new ConstantExpression("spendings%s"),
			new PathExpression(batch.add(CleansFunctions.FIRST), new ObjectAccess("UniqueTransactionID"))));
		fundProjection.addMapping("amount", batch.add(CleansFunctions.SUM, new TernaryExpression(new ObjectAccess(
			"DollarsObligated"), coerce(DecimalNode.class, new ObjectAccess("DollarsObligated")),
			new ConstantExpression(0))));
		fundProjection.addMapping("currency", new ConstantExpression("USD"));
		ObjectCreation date = new ObjectCreation();
		date.addMapping("year", batch.add(CleansFunctions.FIRST, new ObjectAccess("FiscalYear")));
		fundProjection.addMapping("date", date);
		fundProjection.addMapping("subject", new PathExpression(batch.add(CleansFunctions.FIRST), new ObjectAccess(
			"ProductorServiceCode")));

		if (this.scrubbed[SPENDING] == null)
			this.scrubSpending(true);
		Grouping grouping = new Grouping(fundProjection, this.scrubbed[SPENDING]);
		grouping.withKeyProjection(new ObjectAccess("UniqueTransactionID"));

		this.inputs[SPENDING][FUND] = grouping;
		this.sinks
			.add(new Sink(getInternalOutputFormat(), String.format("%s/SpendingFunds.json", this.outputDir), grouping));
	}

	private void mapEarmarkFund(boolean simulate) {
		if (simulate) {
			this.inputs[EARMARK][FUND] = new Source(getInternalInputFormat(), String.format("%s/EarmarkFunds.json",
				this.outputDir));
			return;
		}

		ObjectCreation fundProjection = new ObjectCreation();

		final BatchAggregationExpression batch = new BatchAggregationExpression();
		fundProjection.addMapping("id", new FunctionCall("format", new ConstantExpression("earmark%s"),
			new PathExpression(batch.add(CleansFunctions.FIRST), new ObjectAccess("earmarkId"))));
		fundProjection.addMapping("amount", batch.add(CleansFunctions.SUM, new TernaryExpression(new ObjectAccess(
			"amount"), new ObjectAccess("amount"), new ConstantExpression(0))));
		fundProjection.addMapping("currency", new ConstantExpression("USD"));
		ObjectCreation date = new ObjectCreation();
		date.addMapping("year", batch.add(CleansFunctions.FIRST, new ObjectAccess("enactedYear")));
		fundProjection.addMapping("date", date);
		fundProjection.addMapping("subject", new PathExpression(batch.add(CleansFunctions.FIRST), new ObjectAccess(
			"shortDescription")));

		if (this.scrubbed[EARMARK] == null)
			this.scrubEarmark(true);
		Grouping grouping = new Grouping(fundProjection, this.scrubbed[EARMARK]);
		grouping.withKeyProjection(new ObjectAccess("earmarkId"));

		this.inputs[EARMARK][FUND] = grouping;
		this.sinks.add(new Sink(getInternalOutputFormat(), String.format("%s/EarmarkFunds.json", this.outputDir), grouping));
	}

	private Operator mapEarmarkStates() {
		ObjectCreation projection = new ObjectCreation();
		PathExpression name = new PathExpression(new ArrayAccess(0), new ObjectAccess("sponsorStateCode"));
		projection.addMapping("id", new FunctionCall("format", new ConstantExpression("congressLegalEntity%s"), name));
		projection.addMapping("names", new ArrayCreation(name));

		ObjectCreation address = new ObjectCreation();
		address.addMapping("state", name);
		address.addMapping("country", new ConstantExpression("United States of America"));
		projection.addMapping("addresses", address);

		ComparativeExpression recipientCondition = new ComparativeExpression(new ObjectAccess("sponsorStateCode"),
			BinaryOperator.NOT_EQUAL, new ConstantExpression(""));
		Selection sponsors = new Selection(recipientCondition, this.scrubbed[EARMARK]);
		Grouping grouping = new Grouping(projection, sponsors);
		grouping.withKeyProjection(new ObjectAccess("sponsorStateCode"));
		return grouping;
	}

	private void mapEarmarkLegalEntity(boolean simulate) {
		if (simulate) {
			this.inputs[EARMARK][LEGAL_ENTITY] = new Source(getInternalInputFormat(), String.format(
				"%s/EarmarkLegalEntities.json",
				this.outputDir));
			return;
		}

		if (this.scrubbed[EARMARK] == null)
			this.scrubEarmark(true);
		UnionAll legalEntities = new UnionAll(this.mapEarmarkReceiver(), this.mapEarmarkStates());
		this.inputs[EARMARK][LEGAL_ENTITY] = legalEntities;
		this.sinks.add(new Sink(getInternalOutputFormat(), String.format("%s/EarmarkLegalEntities.json", this.outputDir),
			legalEntities));

	}

	private Operator mapEarmarkReceiver() {

		ObjectCreation projection = new ObjectCreation();

		final BatchAggregationExpression batch = new BatchAggregationExpression();

		projection.addMapping("names", new ArrayCreation(new PathExpression(batch.add(CleansFunctions.FIRST),
			new ObjectAccess("recipient"))));
		projection.addMapping("id", new GenerateExpression("earmarkReceiver%s"));

		ObjectCreation funds = new ObjectCreation();
		funds.addMapping("fund", new FunctionCall("format", new ConstantExpression("earmark%s"), new ObjectAccess(
			"earmarkId")));
		funds.addMapping("amount", new ObjectAccess("currentContractValue"));
		funds.addMapping("currency", new ConstantExpression("USD"));
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
		Selection sponsors = new Selection(recipientCondition, this.scrubbed[EARMARK]);
		Grouping grouping = new Grouping(projection, sponsors);
		grouping.withKeyProjection(new ObjectAccess("recipient"));

		return grouping;
	}

	private void mapEarmarkPerson(boolean simulate) {
		if (simulate) {
			this.inputs[EARMARK][PERSON] = new Source(getInternalInputFormat(), String.format("%s/EarmarkPerson.json",
				this.outputDir));
			return;
		}

		ObjectCreation projection = new ObjectCreation();

		final BatchAggregationExpression batch = new BatchAggregationExpression();

		projection.addMapping("id", new GenerateExpression("earmarkPerson%s"));
		PathExpression first = new PathExpression(batch.add(CleansFunctions.FIRST),
			new ObjectAccess("sponsorFirstName"));
		PathExpression middle = new PathExpression(batch.add(CleansFunctions.FIRST), new ObjectAccess(
			"sponsorMiddleName"));
		projection.addMapping("firstName",
			new TernaryExpression(middle, new FunctionCall("format", new ConstantExpression("%s %s"), first, middle),
				first));
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
		employment.addMapping("position", new ObjectAccess("sponsorHonorific"));

		if (this.scrubbed[EARMARK] == null)
			this.scrubEarmark(true);
		ComparativeExpression sponsorCondition = new ComparativeExpression(new ObjectAccess("recordType"),
			BinaryOperator.EQUAL, new ConstantExpression("C"));
		Selection sponsors = new Selection(sponsorCondition, this.scrubbed[EARMARK]);
		Grouping grouping = new Grouping(projection, sponsors);
		grouping.withKeyProjection(new ArrayCreation(new ObjectAccess("sponsorFirstName"),
			new ObjectAccess("sponsorMiddleName"), new ObjectAccess("sponsorLastName")));

		Operator persons = grouping;
		this.inputs[EARMARK][PERSON] = persons;
		this.sinks.add(
			new Sink(getInternalOutputFormat(), String.format("%s/EarmarkPerson.json", this.outputDir), persons));

	}

	protected Projection getNickNameLookup() {
		Source nickNames = new Source(CsvInputFormat.class, String.format("%s/nicknamesSorted.csv", this.inputDir));
		EvaluationExpression[] realNames = new EvaluationExpression[8];
		for (int index = 0; index < realNames.length; index++)
			realNames[index] = new ObjectAccess(String.format("REALNAME_%d", index + 1));
		EvaluationExpression filteredNames = new FunctionCall("filter", new ArrayCreation(realNames),
			new ConstantExpression(null), new ConstantExpression(""));
		Projection nickNamesToRealNames = new Projection(new ObjectAccess("NICKNAME"), filteredNames, nickNames);
		return nickNamesToRealNames;
	}

	private Operator mapSpendingAgencies() {
		ObjectCreation projection = new ObjectCreation();
		PathExpression name = new PathExpression(new ArrayAccess(0), new ObjectAccess("ContractingAgency"));
		projection.addMapping("id", new FunctionCall("format", new ConstantExpression("spendingLegalEntity%s"), name));
		projection.addMapping("name", name);

		Grouping grouping = new Grouping(projection, this.scrubbed[SPENDING]);
		grouping.withKeyProjection(new ObjectAccess("ContractingAgency"));
		return grouping;
	}

	private void mapSpendingLegalEntity(boolean simulate) {
		if (simulate) {
			this.inputs[SPENDING][LEGAL_ENTITY] = new Source(getInternalInputFormat(), String.format(
				"%s/SpendingLegalEntities.json",
				this.outputDir));
			return;
		}

		if (this.scrubbed[SPENDING] == null)
			this.scrubEarmark(true);
		UnionAll legalEntities = new UnionAll(this.mapSpendingAgencies(), this.mapSpendingReceiver());
		this.inputs[SPENDING][LEGAL_ENTITY] = legalEntities;
		this.sinks.add(new Sink(getInternalOutputFormat(), String.format("%s/SpendingLegalEntities.json", this.outputDir),
			legalEntities));

	}

	private Operator mapSpendingReceiver() {

		ObjectCreation projection = new ObjectCreation();

		final BatchAggregationExpression batch = new BatchAggregationExpression();

		// ArrayCreation namesPerRecord = new ArrayCreation(new ObjectAccess("ParentRecipientOrCompanyName"),
		// new ObjectAccess("RecipientName"),
		// new ObjectAccess("RecipientOrContractorName"),
		// new ObjectAccess("VendorName"));
		//
		projection.addMapping("name", new PathExpression(batch.add(CleansFunctions.FIRST), new ObjectAccess(
			"ParentRecipientOrCompanyName")));
		projection.addMapping("id", new GenerateExpression("spendingReceiver%s"));

		ObjectCreation funds = new ObjectCreation();
		funds.addMapping("fund", new FunctionCall("format", new ConstantExpression("spendings%s"), new ObjectAccess(
			"UniqueTransactionID")));
		funds.addMapping("amount", coerce(DecimalNode.class, new ObjectAccess("DollarsObligated")));
		funds.addMapping("currency", new ConstantExpression("USD"));
		projection.addMapping("receivedFunds", batch.add(CleansFunctions.ALL, funds));

		ObjectCreation address = new ObjectCreation();
		address.addMapping("street", new ObjectAccess("RecipientAddressLine123"));
		address.addMapping("city", new ObjectAccess("RecipientCity"));
		address.addMapping("zipCode", new ObjectAccess("RecipientZipCode"));
		address.addMapping("state", new FunctionCall("camelCase", new ObjectAccess("RecipientState")));
		// address.addMapping("country", new ObjectAccess("vendorCountry"));
		projection.addMapping("addresses", new FunctionCall("distinct", batch.add(CleansFunctions.ALL, address)));

		Grouping grouping = new Grouping(projection, this.scrubbed[SPENDING]);
		grouping.withKeyProjection(new ObjectAccess("ParentRecipientOrCompanyName"));

		return grouping;
	}

	private void scrubSpending(boolean simulate) {
		// if (simulate) {
		// this.scrubbed[SPENDING] = new Source(getInternalInputFormat(), String.format("%s/ScrubbedSpending.json",
		// this.outputDir));
		// return;
		// }

		Source spending = new Source(CsvInputFormat.class, String.format("%s/UsSpending.csv", this.inputDir));
		spending.setParameter(CsvInputFormat.COLUMN_NAMES, new String[] { "UniqueTransactionID", "TransactionStatus",
			"AwardType", "ContractPricing", "ContractingAgency", "DUNSNumber", "parentDUNSNumber", "DateSigned",
			"ContractDescription", "ReasonForModification", "DollarsObligated", "ExtentCompeted", "FiscalYear",
			"TransactionNumber", "AgencyID", "FundingAgency", "IDVAgency", "IDVProcurementInstrumentID", "MajorAgency",
			"ContractingAgencyCode", "MajorFundingAgency", "ModificationNumber", "PSCCategoryCode",
			"ParentRecipientOrCompanyName", "PlaceofPerformanceCongDistrict", "PlaceofPerformanceState",
			"PlaceofPerformanceZipCode", "PrincipalNAICSCode", "ProcurementInstrumentID", "PrincipalPlaceCountyOrCity",
			"ProductorServiceCode", "ProgramSource", "ProgramSourceAccountCode", "ProgramSourceAgencyCode",
			"ProgramSourceDescription", "RecipientAddressLine123", "RecipientCity", "RecipientCongressionalDistrict",
			"RecipientCountyName", "RecipientName", "RecipientOrContractorName", "RecipientState", "RecipientZipCode",
			"TypeofSpending", "TypeofTransaction", "VendorName" });

		// String[] usedFields = new String[] { "contractingAgency", "parentCompanyName",
		// "vendorAlternateName", "vendorDoingBusinessAsName", "vendorLegalOrganizationName",
		// "vendorNameFromContract", "procurementInstrumentId", "idvProcurementInstrumentId",
		// "currentContractValue", "vendorAddressLine1", "vendorAddressLine2", "vendorAddressLine3",
		// "vendorAddressCity", "vendorZipCode", "vendorAddressState", "vendorCountry", "vendorPhoneNumber",
		// "fiscalYear", "contractDescription"};
		//
		// ObjectCreation creation = new ObjectCreation();
		// for (String field : usedFields) {
		// creation.addMapping(field, new ObjectAccess(field));
		// }
		// spending = new Projection(creation, spending);
		//
		// scrubbed = new Lookup(new ObjectAccess("sponsorFirstName"), new Source(getInternalInputFormat(), ));
		// validation.addRule(new RangeRule(new ObjectAccess("memberName")));

		ComparativeExpression withReceiver = new ComparativeExpression(new FunctionCall("length", new ObjectAccess(
			"ParentRecipientOrCompanyName")), BinaryOperator.GREATER, new ConstantExpression(1));
		this.scrubbed[SPENDING] = new Selection(withReceiver, spending);
		// this.sinks.add(new Sink(SequentialOutputFormat.class, String.format("%s/ScrubbedSpending.json", this.outputDir),
		// spending));
	}

	private void scrubEarmark(boolean simulate) {
		if (simulate) {
			this.scrubbed[EARMARK] = new Source(getInternalInputFormat(), String.format("%s/ScrubbedEarmarks.json",
				this.outputDir));
			return;
		}

		Source earmarks = new Source(PersistenceType.HDFS,
			String.format("%s/OriginalUsEarmark2008.json", this.inputDir));
		Operator scrubbed = new Validation(earmarks);

		// scrubbed = new Lookup(new ObjectAccess("sponsorFirstName"), new Source(getInternalInputFormat(), ));
		// validation.addRule(new RangeRule(new ObjectAccess("memberName")));
		this.scrubbed[EARMARK] = scrubbed;
		this.sinks.add(new Sink(getInternalOutputFormat(), String.format("%s/ScrubbedEarmarks.json", this.outputDir),
			scrubbed));
	}


	private Operator[] scrubbed = new Operator[4];

	private Operator joinedCongress;

	private void scrubCongress(boolean simulate) {
		if (simulate) {
			this.scrubbed[CONGRESS] = new Source(getInternalInputFormat(), String.format("%s/ScrubbedCongress.json",
				this.outputDir));
			return;
		}

		if (joinedCongress == null)
			this.joinCongress(true);
		Validation scrubbedCongress = new Validation(joinedCongress);
		scrubbedCongress.addRule(new NonNullRule(new ObjectAccess("memberName")));
		scrubbedCongress.addRule(new NonNullRule(TextNode.valueOf("none"), new ObjectAccess("biography")));
		scrubbedCongress.addRule(new PatternValidationExpression(Pattern.compile("[0-9]+\\(.*"), new ObjectAccess(
			"congresses"), new ArrayAccess(-1), new ObjectAccess("number")));
		scrubbedCongress
			.addRule(new BlackListRule(Arrays.asList(TextNode.valueOf("NA")), TextNode.valueOf(""), new ObjectAccess(
				"congress"), new ArrayProjection(new ObjectAccess("position"))));
		this.scrubbed[CONGRESS] = scrubbedCongress;

		this.sinks.add(new Sink(getInternalOutputFormat(), String.format("%s/ScrubbedCongress.json", this.outputDir),
			scrubbedCongress));
	}

	private void joinCongress(boolean simulate) {
		if (simulate) {
			joinedCongress = new Source(getInternalInputFormat(), String.format("%s/JoinedCongress.json", this.outputDir));
			return;
		}

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
			new ObjectAccess("biography", true)));

		Grouping congress = new Grouping(resultProjection,
			new Source(PersistenceType.HDFS, String.format("%s/OriginalUsCongress.json", this.inputDir)),
			new Source(PersistenceType.HDFS, String.format("%s/OriginalUsCongressBiography.json", this.inputDir)));

		congress.withKeyProjection(0, new ObjectAccess("biography"));
		congress.withKeyProjection(1, new ObjectAccess("id"));

		joinedCongress = new Selection(new UnaryExpression(new ObjectAccess("biography")), congress);
		this.sinks.add(new Sink(getInternalOutputFormat(), String.format("%s/JoinedCongress.json", this.outputDir),
			congress));
	}

	public static void main(String[] args) {
		int step = 3;
		new GovWild().getPlan("1", "", "");// , String.valueOf(step), String.valueOf(step + 1));

		// File dir = new File("/home/arv/workflow/input");
		// for (File file : dir.listFiles())
		// if (file.getName().contains("Free"))
		// try {
		// final JsonParser parser = JsonUtil.FACTORY.createJsonParser(file);
		// parser.setCodec(JsonUtil.OBJECT_MAPPER);
		// parser.readValueAsTree();
		// } catch (Exception e) {
		// System.out.println(file + " not parsed" + e);
		// }
	}

	@Override
	public String getDescription() {
		return "Parameters: [noSubStasks] [inputDir] [outputDir]";
	}
	

	protected Class<? extends FileInputFormat<Key, PactJsonObject>> getInternalInputFormat() {
		return JsonInputFormat.class;
	}

	protected Class<? extends FileOutputFormat<PactJsonObject.Key, PactJsonObject>> getInternalOutputFormat() {
		return JsonOutputFormat.class;
	}

//	public static class InputFo extends SequentialInputFormat<PactJsonObject.Key, PactJsonObject> {
//	};
//	
//	public static class OutputFo extends SequentialOutputFormat {
//		public OutputFo() {
//			try {
//				Field declaredField = SequentialOutputFormat.class.getDeclaredField("typesWritten");
//				declaredField.setAccessible(true);
//				declaredField.setBoolean(this, true);
//			} catch ( Exception e) {
//				throw new RuntimeException(e);
//			}
//		}		
//	};
}
