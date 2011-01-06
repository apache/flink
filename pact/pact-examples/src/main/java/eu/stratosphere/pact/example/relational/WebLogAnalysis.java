/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.example.relational;

import java.util.Iterator;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.contract.OutputContract.UniqueKey;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.example.relational.util.StringTupleDataInFormat;
import eu.stratosphere.pact.example.relational.util.StringTupleDataOutFormat;
import eu.stratosphere.pact.example.relational.util.Tuple;

/**
 * Implements the following relational OLAP query as PACT program:
 * 
 * <code><pre>
 * SELECT r.pageURL, r.pageRank, r.avgDuration
 * FROM Documents d JOIN Rankings r
 * 	ON d.url = r.url
 * WHERE CONTAINS(d.text, [keywords])
 * 	AND r.rank > [rank]
 * 	AND NOT EXISTS (
 * 		SELECT * FROM Visits v
 * 		WHERE v.url = d.url
 * 			AND v.visitDate < 2000-01-01); 
 * </pre></code> Table Schemas: <code><pre>
 * CREATE TABLE Documents (
 * 	url VARCHAR(100) PRIMARY KEY,
 * 	contents TEXT );
 * 
 * CREATE TABLE Rankings (
 * 	pageRank INT,
 * 	pageURL VARCHAR(100) PRIMARY KEY,     
 * 	avgDuration INT );       
 * 
 * CREATE TABLE UserVisits (
 * 	sourceIP VARCHAR(16),
 * 	destURL VARCHAR(100),
 * 	visitDate DATE,
 * 	adRevenue FLOAT,
 * 	userAgent VARCHAR(64),
 * 	countryCode VARCHAR(3),
 * 	languageCode VARCHAR(6),
 * 	searchWord VARCHAR(32),
 * 	duration INT );
 * </pre></code>
 */

public class WebLogAnalysis implements PlanAssembler, PlanAssemblerDescription {

	/**
	 * Input contract
	 */

	@SameKey
	public static class FilterDocs extends
			MapStub<PactString, Tuple, PactString, Tuple> {
		// TODO find appropriate threshold
		/*
		 * private static final String[] KEYWORDS = { "cloud", "atomic",
		 * "cosmic", "universe", "chromospheres", "statistics", "resolution",
		 * "theory", "data", "extraterrestrial", "atoms", "kinematics" };
		 */
		private static final String[] KEYWORDS = { "editors", "oscillations",
				"convection" };

		@Override
		protected void map(PactString key, Tuple value,
				Collector<PactString, Tuple> out) {
			// FILTER
			// Only collect the document if all keywords are contained
			String docText = value.getStringValueAt(1);
			boolean allContained = true;
			for (String kw : KEYWORDS) {
				if (!docText.contains(kw)) {
					allContained = false;
					break;
				}
			}

			if (allContained) {
				out.collect(key, new Tuple());
			}
		}
	}

	/**
	 * Input contract 
	 * TODO: add javadoc
	 * 
	 */

	public static class FilterRanks extends
			MapStub<PactString, Tuple, PactString, Tuple> {

		// TODO find appropriate threshold
		// private static final int rankFilter = 50;
		private static final int rankFilter = 0;

		@Override
		protected void map(PactString key, Tuple value,
				Collector<PactString, Tuple> out) {
			// FILTER
			int rank = (int) value.getLongValueAt(0);
			if (rank > rankFilter) {

				PactString urlKey = new PactString(value.getStringValueAt(1));

				out.collect(urlKey, value);
			}
		}
	}

	/**
	 * Input contract TODO: add javadoc
	 */

	public static class FilterVisits extends
			MapStub<PactString, Tuple, PactString, Tuple> {

		// TODO find appropriate threshold
		// private static final int yearFilter = 2010;
		private static final int yearFilter = 1970;

		@Override
		protected void map(PactString key, Tuple value,
				Collector<PactString, Tuple> out) {
			// FILTER
			if (Integer.parseInt(value.getStringValueAt(2).substring(6, 10)) < yearFilter) {
				out.collect(new PactString(value.getStringValueAt(1)),
						new Tuple());
			}
		}
	}

	/**
	 * Input contract Output contract @SameKey TODO: add javadoc
	 */
	@SameKey
	public static class JoinDocRanks extends
			MatchStub<PactString, Tuple, Tuple, PactString, Tuple> {

		@Override
		public void match(PactString key, Tuple value1, Tuple value2,
				Collector<PactString, Tuple> out) {
			out.collect(key, value2);
		}
	}

	/**
	 * Input contract Output contract @SameKey TODO: add javadoc
	 */
	@SameKey
	public static class AntiJoinVisits extends
			CoGroupStub<PactString, Tuple, Tuple, PactString, Tuple> {

		@Override
		public void coGroup(PactString key, Iterator<Tuple> values1,
				Iterator<Tuple> values2, Collector<PactString, Tuple> out) {
			if (!values1.hasNext()) {
				while (values2.hasNext()) {
					out.collect(key, values2.next());
				}
			}
		}
	}

	@Override
	public Plan getPlan(String... args) {

		// check for the correct number of job parameters
		if (args.length != 5) {
			// Default parameter
			args = new String[5];
			args[0] = "1";
			args[1] = "hdfs://localhost:9000/demo/webLog/docs_new";
			args[2] = "hdfs://localhost:9000/demo/webLog/ranks_new";
			args[3] = "hdfs://localhost:9000/demo/webLog/visits_new";
			args[4] = "hdfs://localhost:9000/demo/result";
		}

		int noSubTasks = Integer.parseInt(args[0]);
		String docsInput = args[1];
		String ranksInput = args[2];
		String visitsInput = args[3];
		String output = args[4];

		DataSourceContract<PactString, Tuple> docs = new DataSourceContract<PactString, Tuple>(
				StringTupleDataInFormat.class, docsInput, "Documents");
		docs.setFormatParameter(TextInputFormat.FORMAT_PAIR_DELIMITER, "\n");
		docs.setDegreeOfParallelism(noSubTasks);
		docs.setOutputContract(UniqueKey.class);

		DataSourceContract<PactString, Tuple> ranks = new DataSourceContract<PactString, Tuple>(
				StringTupleDataInFormat.class, ranksInput, "Ranks");
		ranks.setFormatParameter(TextInputFormat.FORMAT_PAIR_DELIMITER, "\n");
		ranks.setDegreeOfParallelism(noSubTasks);

		DataSourceContract<PactString, Tuple> visits = new DataSourceContract<PactString, Tuple>(
				StringTupleDataInFormat.class, visitsInput, "Visits");
		visits.setFormatParameter(TextInputFormat.FORMAT_PAIR_DELIMITER, "\n");
		visits.setDegreeOfParallelism(noSubTasks);

		MapContract<PactString, Tuple, PactString, Tuple> filterDocs = new MapContract<PactString, Tuple, PactString, Tuple>(
				FilterDocs.class, "Filter Docs");
		filterDocs.setDegreeOfParallelism(noSubTasks);
		filterDocs.getCompilerHints().setSelectivity(0.15f);
		filterDocs.getCompilerHints().setAvgBytesPerRecord(60);

		MapContract<PactString, Tuple, PactString, Tuple> filterRanks = new MapContract<PactString, Tuple, PactString, Tuple>(
				FilterRanks.class, "Filter Ranks");
		filterRanks.setDegreeOfParallelism(noSubTasks);
		filterRanks.getCompilerHints().setSelectivity(0.25f);

		MapContract<PactString, Tuple, PactString, Tuple> filterVisits = new MapContract<PactString, Tuple, PactString, Tuple>(
				FilterVisits.class, "Filter Visits");
		filterVisits.setDegreeOfParallelism(noSubTasks);
		filterVisits.getCompilerHints().setAvgBytesPerRecord(60);
		filterVisits.getCompilerHints().setSelectivity(0.2f);

		MatchContract<PactString, Tuple, Tuple, PactString, Tuple> joinDocsRanks = new MatchContract<PactString, Tuple, Tuple, PactString, Tuple>(
				JoinDocRanks.class, "Join DocRanks");
		joinDocsRanks.setDegreeOfParallelism(noSubTasks);
		joinDocsRanks.getCompilerHints().setSelectivity(0.15f);

		CoGroupContract<PactString, Tuple, Tuple, PactString, Tuple> antiJoinVisits = new CoGroupContract<PactString, Tuple, Tuple, PactString, Tuple>(
				AntiJoinVisits.class, "Antijoin DocsVisits");
		antiJoinVisits.setDegreeOfParallelism(noSubTasks);

		DataSinkContract<PactString, Tuple> result = new DataSinkContract<PactString, Tuple>(
				StringTupleDataOutFormat.class, output, "Result");
		result.setDegreeOfParallelism(noSubTasks);

		result.setInput(antiJoinVisits);
		antiJoinVisits.setFirstInput(filterVisits);
		filterVisits.setInput(visits);
		antiJoinVisits.setSecondInput(joinDocsRanks);
		joinDocsRanks.setFirstInput(filterDocs);
		filterDocs.setInput(docs);
		joinDocsRanks.setSecondInput(filterRanks);
		filterRanks.setInput(ranks);

		return new Plan(result, "Weblog Analysis");
	}

	@Override
	public String getDescription() {
		return "Parameters: dop, docs-input, ranks-input, visits-input, result-output";
	}

}
