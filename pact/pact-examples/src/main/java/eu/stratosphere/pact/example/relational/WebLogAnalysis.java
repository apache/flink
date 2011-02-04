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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
import eu.stratosphere.pact.common.type.base.PactNull;
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
 *  * </pre></code> 
 * 
 * Table Schemas: <code><pre>
 * CREATE TABLE Documents (
 * 					url VARCHAR(100) PRIMARY KEY,
 * 					contents TEXT );
 * 
 * CREATE TABLE Rankings (
 * 					pageRank INT,
 * 					pageURL VARCHAR(100) PRIMARY KEY,     
 * 					avgDuration INT );       
 * 
 * CREATE TABLE Visits (
 * 					sourceIP VARCHAR(16),
 * 					destURL VARCHAR(100),
 * 					visitDate DATE,
 * 					adRevenue FLOAT,
 * 					userAgent VARCHAR(64),
 * 					countryCode VARCHAR(3),
 * 					languageCode VARCHAR(6),
 * 					searchWord VARCHAR(32),
 * 					duration INT );
 * </pre></code>
 * 
 * @author Fabian Hueske
 */
public class WebLogAnalysis implements PlanAssembler, PlanAssemblerDescription {

	/**
	 * MapStub that filters for documents that contain a certain set of
	 * keywords. 
	 * SameKey is annotated because the key is not changed 
	 * (key of the input set is equal to the key of the output set).
	 * 
	 * @author Fabian Hueske
	 */
	@SameKey
	public static class FilterDocs extends MapStub<PactString, Tuple, PactString, PactNull> {
		
		private static final Log LOG = LogFactory.getLog(FilterDocs.class);
		
		private static final String[] KEYWORDS = { " editors ", " oscillations ", " convection " };

		/**
		 * Filters for documents that contain all of the given keywords and emit their keys.
		 */
		@Override
		protected void map(PactString url, Tuple docRecord, Collector<PactString, PactNull> out) {
			
			// FILTER
			// Only collect the document if all keywords are contained
			String docText = docRecord.getStringValueAt(1);
			boolean allContained = true;
			for (String kw : KEYWORDS) {
				if (!docText.contains(kw)) {
					allContained = false;
					break;
				}
			}

			if (allContained) {
				out.collect(url, new PactNull());
				
				LOG.debug("Emit key: "+url);
			}
		}
	}

	/**
	 * MapStub that filters for records where the rank exceeds a certain threshold.
	 * 
	 * @author Fabian Hueske
	 */
	public static class FilterRanks extends MapStub<PactString, Tuple, PactString, Tuple> {
		
		private static final Log LOG = LogFactory.getLog(FilterRanks.class);

		private static final int rankFilter = 50;

		/**
		 * Filters for records of the rank relation where the rank is greater
		 * than the given threshold. The key is set to the URL of the record.
		 */
		@Override
		protected void map(PactString key, Tuple rankRecord, Collector<PactString, Tuple> out) {
			
			// Extract rank from record 
			int rank = (int) rankRecord.getLongValueAt(0);
			if (rank > rankFilter) {
				
				// create new key and emit key-value-pair
				PactString url = new PactString(rankRecord.getStringValueAt(1));
				out.collect(url, rankRecord);
	
				LOG.debug("Emit: "+url+" , "+rankRecord);
			}
		}
	}

	/**
	 * MapStub that filters for records of the visits relation where the year
	 * (from the date string) is equal to a certain value.
	 * 
	 * @author Fabian Hueske
	 */
	public static class FilterVisits extends MapStub<PactString, Tuple, PactString, PactNull> {

		private static final Log LOG = LogFactory.getLog(FilterVisits.class);
		
		private static final int yearFilter = 2010;

		/**
		 * Filters for records of the visits relation where the year of visit is equal to a
		 * specified value. The URL of all visit records passing the filter is emitted.
		 */
		@Override
		protected void map(PactString key, Tuple visitRecord, Collector<PactString, PactNull> out) {
			
			// Parse date string with the format YYYY-MM-DD and extract the year
			int year = Integer.parseInt(visitRecord.getStringValueAt(2).substring(0,4));
			if (year == yearFilter) {
				
				// Extract the URL from the record and emit it
				PactString url = new PactString(visitRecord.getStringValueAt(1));
				out.collect(url, new PactNull());
				
				LOG.debug("Emit key: "+url);
			}
		}
	}

	/**
	 * MatchStub that joins the filtered entries from the documents and the
	 * ranks relation. SameKey is annotated because the key of both inputs and
	 * the output is the same (URL).
	 * 
	 * @author Fabian Hueske
	 */
	@SameKey
	public static class JoinDocRanks extends MatchStub<PactString, Tuple, PactNull, PactString, Tuple> {

		/**
		 * Collects all entries from the documents and ranks relation where the
		 * key (URL) is identical. The output consists of the old key (URL) and
		 * the attributes of the ranks relation.
		 */
		@Override
		public void match(PactString url, Tuple ranks, PactNull docs, Collector<PactString, Tuple> out) {
			
			// emit the key and the rank value
			out.collect(url, ranks);
		}
	}

	/**
	 * CoGroupStub that realizes an anti-join.
	 * If the first input does not provide any pairs, all pairs of the second input are emitted.
	 * Otherwise, no pair is emitted.
	 * 
	 * @author Fabian Hueske
	 */
	@SameKey
	public static class AntiJoinVisits extends CoGroupStub<PactString, PactNull, Tuple, PactString, Tuple> {

		/**
		 * If the visit iterator is empty, all pairs of the rank iterator are emitted.
		 * Otherwise, no pair is emitted. 
		 */
		@Override
		public void coGroup(PactString url, Iterator<PactNull> visits, Iterator<Tuple> ranks, Collector<PactString, Tuple> out) {
			// Check if there is a entry in the visits relation
			if (!visits.hasNext()) {
				while (ranks.hasNext()) {
					// Emit all rank pairs
					out.collect(url, ranks.next());
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
			throw new IllegalArgumentException(
				"Must provide five arguments: <parallelism> <docs_input> <ranks_input> <visits_input> <result_directory>");
		}

		// parse job parameters
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
		MapContract<PactString, Tuple, PactString, PactNull> filterDocs = new MapContract<PactString, Tuple, PactString, PactNull>(
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
		MapContract<PactString, Tuple, PactString, PactNull> filterVisits = new MapContract<PactString, Tuple, PactString, PactNull>(
				FilterVisits.class, "Filter Visits");
		filterVisits.setDegreeOfParallelism(noSubTasks);
		filterVisits.getCompilerHints().setAvgBytesPerRecord(60);
		filterVisits.getCompilerHints().setSelectivity(0.2f);

		// Create MatchContract to join the filtered documents and ranks
		// relation
		MatchContract<PactString, Tuple, PactNull, PactString, Tuple> joinDocsRanks = new MatchContract<PactString, Tuple, PactNull, PactString, Tuple>(
				JoinDocRanks.class, "Join DocRanks");
		joinDocsRanks.setDegreeOfParallelism(noSubTasks);
		joinDocsRanks.getCompilerHints().setSelectivity(0.15f);

		// Create CoGroupContract to realize a anti join between the joined
		// documents and ranks relation and the filtered visits relation
		CoGroupContract<PactString, PactNull, Tuple, PactString, Tuple> antiJoinVisits = new CoGroupContract<PactString, PactNull, Tuple, PactString, Tuple>(
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
		joinDocsRanks.setFirstInput(filterRanks);
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
