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
 * 		WHERE v.destUrl = d.url
 * 			AND v.visitDate < [date]); 
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
 * CREATE TABLE Visits (
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
 * 
 * @author Fabian Hueske
 */
public class WebLogAnalysis implements PlanAssembler, PlanAssemblerDescription {

	/**
	 * Map PACT that collects all documents that contain a certain set of
	 * keywords. SameKey is annotated because the key does not change while
	 * processing (key of the input set is equal to the key of the output set).
	 * 
	 * @author Fabian Hueske
	 */
	@SameKey
	public static class FilterDocs extends
			MapStub<PactString, Tuple, PactString, Tuple> {
		
		// TODO change the keywords for the docs relation here
		private static final String[] KEYWORDS = { "editors", "oscillations",
				"convection" };

		/**
		 * Collects the documents that contain all of the given keywords.
		 */
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
				// Only keep the key
				out.collect(key, new Tuple());
			}
		}
	}

	/**
	 * Map PACT that collects all entries of the rank relation where rank
	 * exceeds a certain threshold.
	 * 
	 * @author Fabian Hueske
	 */
	public static class FilterRanks extends
			MapStub<PactString, Tuple, PactString, Tuple> {

		// TODO change the filter value for the ranks relation here
		private static final int rankFilter = 50;

		/**
		 * Collects all entries from the rank relation where the rank is greater
		 * than the given threshold.
		 */
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
	 * Map PACT that collects all entries of the visits relation where the year
	 * (from the date string) is equal to a certain filter value.
	 * 
	 * @author Fabian Hueske
	 */
	public static class FilterVisits extends
			MapStub<PactString, Tuple, PactString, Tuple> {

		// TODO change the filter value for the visits relation here
		private static final int yearFilter = 2010;

		/**
		 * Parses the date string in order to get the year and collects all
		 * entries where year is equal to the given filter value.
		 */
		@Override
		protected void map(PactString key, Tuple value,
				Collector<PactString, Tuple> out) {
			// FILTER
			// Parses a datestring with the format YYYY-MM-DD
			int year = Integer.parseInt(value.getStringValueAt(2).substring(0,
					4));
			if (year == yearFilter) {
				// Keep only the second value (URL) as key and an empty tuple
				// as value
				out.collect(new PactString(value.getStringValueAt(1)),
						new Tuple());
			}
		}
	}

	/**
	 * Match PACT that joins the filtered entries from the documents and the
	 * ranks relation. SameKey is annotated because the key of both inputs and
	 * the output is the same (URL).
	 * 
	 * @author Fabian Hueske
	 */
	@SameKey
	public static class JoinDocRanks extends
			MatchStub<PactString, Tuple, Tuple, PactString, Tuple> {

		/**
		 * Collects all entries from the documents and ranks relation where the
		 * key (URL) is identical. The output consists of the old key (URL) and
		 * the attributes of the ranks relation.
		 */
		@Override
		public void match(PactString key, Tuple value1, Tuple value2,
				Collector<PactString, Tuple> out) {
			// Just keep the key and the second value
			// The second value is associated with the ranks relation
			out.collect(key, value2);
		}
	}

	/**
	 * CoGroup PACT that realizes the anti join from the OLAP query. Takes the
	 * FilterVisits as first and the JoinDocRanks as second input. The entries
	 * from the JoinDocRanks relation are only collected if there is no
	 * corresponding entry (with the same key/ URL) in the FilterVisits
	 * relation.
	 * 
	 * @author Fabian Hueske
	 */
	@SameKey
	public static class AntiJoinVisits extends
			CoGroupStub<PactString, Tuple, Tuple, PactString, Tuple> {

		/**
		 * Collects all entries from the joined documents and ranks relations
		 * where no corresponding entry in the filtered visits relation exists.
		 * The CoGroup PACT guarantees that all key/ value pairs from the input
		 * sets are grouped in one result set. Hence only the values are
		 * collected where no visits entry exists (the second value is empty).
		 */
		@Override
		public void coGroup(PactString key, Iterator<Tuple> values1,
				Iterator<Tuple> values2, Collector<PactString, Tuple> out) {
			// Check if there is a entry in the visits relation
			// Continue only if there is no entry
			if (!values1.hasNext()) {
				while (values2.hasNext()) {
					// Collect all attributes from the joined documents and rank
					// relation.
					out.collect(key, values2.next());
				}
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(String... args) {

		// check for the correct number of job parameters
		if (args.length != 5) {
			// Default parameter
			args = new String[5];
			args[0] = "1"; // number of subtasks
			args[1] = "hdfs://localhost:9000/examples/webLog/docs";
			args[2] = "hdfs://localhost:9000/examples/webLog/ranks";
			args[3] = "hdfs://localhost:9000/examples/webLog/visits";
			args[4] = "hdfs://localhost:9000/examples/results/webLog";
		}

		int noSubTasks = Integer.parseInt(args[0]);
		String docsInput = args[1];
		String ranksInput = args[2];
		String visitsInput = args[3];
		String output = args[4];

		// Create DataSourceContract for documents relation
		DataSourceContract<PactString, Tuple> docs = new DataSourceContract<PactString, Tuple>(
				StringTupleDataInFormat.class, docsInput, "Documents");
		docs.setFormatParameter(TextInputFormat.FORMAT_PAIR_DELIMITER, "\n");
		docs.setDegreeOfParallelism(noSubTasks);
		docs.setOutputContract(UniqueKey.class);

		// Create DataSourceContract for ranks relation
		DataSourceContract<PactString, Tuple> ranks = new DataSourceContract<PactString, Tuple>(
				StringTupleDataInFormat.class, ranksInput, "Ranks");
		ranks.setFormatParameter(TextInputFormat.FORMAT_PAIR_DELIMITER, "\n");
		ranks.setDegreeOfParallelism(noSubTasks);

		// Create DataSourceContract for visits relation
		DataSourceContract<PactString, Tuple> visits = new DataSourceContract<PactString, Tuple>(
				StringTupleDataInFormat.class, visitsInput, "Visits");
		visits.setFormatParameter(TextInputFormat.FORMAT_PAIR_DELIMITER, "\n");
		visits.setDegreeOfParallelism(noSubTasks);

		// Create MapContract for filtering the entries from the documents
		// relation
		MapContract<PactString, Tuple, PactString, Tuple> filterDocs = new MapContract<PactString, Tuple, PactString, Tuple>(
				FilterDocs.class, "Filter Docs");
		filterDocs.setDegreeOfParallelism(noSubTasks);
		filterDocs.getCompilerHints().setSelectivity(0.15f);
		filterDocs.getCompilerHints().setAvgBytesPerRecord(60);

		// Create MapContract for filtering the entries from the ranks relation
		MapContract<PactString, Tuple, PactString, Tuple> filterRanks = new MapContract<PactString, Tuple, PactString, Tuple>(
				FilterRanks.class, "Filter Ranks");
		filterRanks.setDegreeOfParallelism(noSubTasks);
		filterRanks.getCompilerHints().setSelectivity(0.25f);

		// Create MapContract for filtering the entries from the visits relation
		MapContract<PactString, Tuple, PactString, Tuple> filterVisits = new MapContract<PactString, Tuple, PactString, Tuple>(
				FilterVisits.class, "Filter Visits");
		filterVisits.setDegreeOfParallelism(noSubTasks);
		filterVisits.getCompilerHints().setAvgBytesPerRecord(60);
		filterVisits.getCompilerHints().setSelectivity(0.2f);

		// Create MatchContract to join the filtered documents and ranks
		// relation
		MatchContract<PactString, Tuple, Tuple, PactString, Tuple> joinDocsRanks = new MatchContract<PactString, Tuple, Tuple, PactString, Tuple>(
				JoinDocRanks.class, "Join DocRanks");
		joinDocsRanks.setDegreeOfParallelism(noSubTasks);
		joinDocsRanks.getCompilerHints().setSelectivity(0.15f);

		// Create CoGroupContract to realize a anti join between the joined
		// documents and ranks relation and the filtered visits relation
		CoGroupContract<PactString, Tuple, Tuple, PactString, Tuple> antiJoinVisits = new CoGroupContract<PactString, Tuple, Tuple, PactString, Tuple>(
				AntiJoinVisits.class, "Antijoin DocsVisits");
		antiJoinVisits.setDegreeOfParallelism(noSubTasks);

		// Create DataSinkContract for writing the result of the OLAP query
		DataSinkContract<PactString, Tuple> result = new DataSinkContract<PactString, Tuple>(
				StringTupleDataOutFormat.class, output, "Result");
		result.setDegreeOfParallelism(noSubTasks);

		// Assemble plan
		filterDocs.setInput(docs);
		filterRanks.setInput(ranks);
		filterVisits.setInput(visits);
		joinDocsRanks.setFirstInput(filterDocs);
		joinDocsRanks.setSecondInput(filterDocs);
		antiJoinVisits.setFirstInput(filterVisits);
		antiJoinVisits.setSecondInput(joinDocsRanks);
		result.setInput(antiJoinVisits);

		// Return the PACT plan
		return new Plan(result, "Weblog Analysis");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDescription() {
		return "Parameters: [noSubTasks], [docsInput], [ranksInput], [visitsInput], [output]";
	}

}
