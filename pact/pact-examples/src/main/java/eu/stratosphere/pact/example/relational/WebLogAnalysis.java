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

import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.util.FieldSet;

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
	 * Converts a input line, assuming to contain a string, into a record that has a single field,
	 * which is a {@link PactString}, containing that line.
	 */
	public static class LineInFormat extends DelimitedInputFormat
	{
		private final PactString string = new PactString();
		
		@Override
		public boolean readRecord(PactRecord record, byte[] line, int numBytes)
		{
			this.string.setValueAscii(line, 0, numBytes);
			record.setField(0, this.string);
			return true;
		}
	}
	
	// TODO JAVADOC !!!
	public static class WebLogAnalysisOutFormat extends FileOutputFormat {
		
		private final StringBuilder buffer = new StringBuilder();

		@Override
		public void writeRecord(PactRecord record) throws IOException {
			buffer.setLength(0);
			buffer.append(record.getField(1, PactInteger.class).toString());
			buffer.append('|');
			buffer.append(record.getField(0, PactString.class).toString());
			buffer.append('|');
			buffer.append(record.getField(2, PactInteger.class).toString());
			buffer.append('|');
			buffer.append('\n');
			byte[] bytes = this.buffer.toString().getBytes();
			this.stream.write(bytes);
		}
	}

	/**
	 * MapStub that filters for documents that contain a certain set of
	 * keywords. 
	 * SameKey is annotated because the key is not changed 
	 * (key of the input set is equal to the key of the output set).
	 * 
	 * @author Fabian Hueske
	 * @author Christoph Bruecke
	 */
	// TODO annotate with SameKey
	public static class FilterDocs extends MapStub {
		
		private static final Log LOG = LogFactory.getLog(FilterDocs.class);		
		private static final String[] KEYWORDS = { " editors ", " oscillations ", " convection " };
		
		private final PactString string = new PactString();
		private final PactString url = new PactString();

		/**
		 * Filters for documents that contain all of the given keywords and emit their keys.
		 */
		@Override
		public void map(PactRecord record, Collector out) throws Exception {
			this.string.setValue(record.getField(0, PactString.class));
			String[] fields = this.string.getValue().split("\\|");
			this.url.setValue(fields[0]);
			
			// FILTER
			// Only collect the document if all keywords are contained
			String docText = fields[1];
			boolean allContained = true;
			for (String kw : KEYWORDS) {
				if (!docText.contains(kw)) {
					allContained = false;
					break;
				}
			}

			if (allContained) {
				record.setField(0, this.url);
				out.collect(record);
				
				LOG.debug("Emit key: " + url);
			}
		}
	}

	/**
	 * MapStub that filters for records where the rank exceeds a certain threshold.
	 * 
	 * @author Fabian Hueske
	 * @author Christoph Bruecke
	 */
	public static class FilterRanks extends MapStub {
		
		private static final Log LOG = LogFactory.getLog(FilterRanks.class);
		private static final int RANKFILTER = 50;
		
		private final PactInteger rank = new PactInteger();
		private final PactString url = new PactString();
		private final PactInteger avgDuration = new PactInteger();

		/**
		 * Filters for records of the rank relation where the rank is greater
		 * than the given threshold. The key is set to the URL of the record.
		 */
		@Override
		public void map(PactRecord record, Collector out) throws Exception {
			//PactInteger rank = record.getField(0, PactInteger.class);
			String[] fields = record.getField(0, PactString.class).getValue().split("\\|");
			this.rank.setValue(Integer.valueOf(fields[0]));
			this.url.setValue(fields[1]);
			this.avgDuration.setValue(Integer.valueOf(fields[2]));
			
			if (this.rank.getValue() > RANKFILTER) {
				// create new key and emit key-value-pair
				record.setNumFields(3);
				record.setField(0, this.url);
				record.setField(1, this.rank);
				record.setField(2, this.avgDuration);
				out.collect(record);
	
				LOG.debug("Emit: " + this.url.getValue() + " , " + this.rank.getValue() + "," + this.avgDuration.getValue());
			}			
		}
	}

	/**
	 * MapStub that filters for records of the visits relation where the year
	 * (from the date string) is equal to a certain value.
	 * 
	 * @author Fabian Hueske
	 * @author Christoph Bruecke
	 */
	public static class FilterVisits extends MapStub {

		private static final Log LOG = LogFactory.getLog(FilterVisits.class);		
		private static final int YEARFILTER = 2010;
		
		private final PactString url = new PactString();

		/**
		 * Filters for records of the visits relation where the year of visit is equal to a
		 * specified value. The URL of all visit records passing the filter is emitted.
		 */
		@Override
		public void map(PactRecord record, Collector out) throws Exception {
			String[] fields = record.getField(0, PactString.class).getValue().split("\\|");
			this.url.setValue(fields[1]);
			
			// Parse date string with the format YYYY-MM-DD and extract the year
			String dateString = fields[2];
			int year = Integer.parseInt(dateString.substring(0,4)); 
			
			if (year == YEARFILTER) {
				record.setNumFields(1);
				record.setField(0, this.url);
				out.collect(record);
				
				LOG.debug("Emit: " + this.url + "(year == " + year + ")");
			}
		}
	}

	/**
	 * MatchStub that joins the filtered entries from the documents and the
	 * ranks relation. SameKey is annotated because the key of both inputs and
	 * the output is the same (URL).
	 * 
	 * @author Fabian Hueske
	 * @author Christoph Bruecke
	 */
	// TODO annotate with SameKey
	public static class JoinDocRanks extends MatchStub {

		/**
		 * Collects all entries from the documents and ranks relation where the
		 * key (URL) is identical. The output consists of the old key (URL) and
		 * the attributes of the ranks relation.
		 */
		@Override
		public void match(PactRecord document, PactRecord rank, Collector out) throws Exception {
			out.collect(rank);	
		}
	}

	/**
	 * CoGroupStub that realizes an anti-join.
	 * If the first input does not provide any pairs, all pairs of the second input are emitted.
	 * Otherwise, no pair is emitted.
	 * 
	 * @author Fabian Hueske
	 * @author Christoph Bruecke
	 */
	public static class AntiJoinVisits extends CoGroupStub {

		/**
		 * If the visit iterator is empty, all pairs of the rank iterator are emitted.
		 * Otherwise, no pair is emitted. 
		 */
		@Override
		public void coGroup(Iterator<PactRecord> ranks, Iterator<PactRecord> visits, Collector out) {
			// Check if there is a entry in the visits relation
			if (!visits.hasNext()) {
				while (ranks.hasNext()) {
					// Emit all rank pairs
					out.collect(ranks.next());
				}
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(String... args) {

		// parse job parameters
		int noSubTasks     = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String docsInput   = (args.length > 1 ? args[1] : "");
		String ranksInput  = (args.length > 2 ? args[2] : "");
		String visitsInput = (args.length > 3 ? args[3] : "");
		String output      = (args.length > 4 ? args[4] : "");

		// Create DataSourceContract for documents relation
		FileDataSource docs = new FileDataSource(LineInFormat.class, docsInput, "Docs Input");
		docs.setDegreeOfParallelism(noSubTasks);
		// TODO set Unique Key contract
		// docs.setOutputContract(UniqueKey.class);

		// Create DataSourceContract for ranks relation
		FileDataSource ranks = new FileDataSource(LineInFormat.class, ranksInput, "Ranks input");
		ranks.setDegreeOfParallelism(noSubTasks);

		// Create DataSourceContract for visits relation
		FileDataSource visits = new FileDataSource(LineInFormat.class, visitsInput, "Visits input:q");
		visits.setDegreeOfParallelism(noSubTasks);

		// Create MapContract for filtering the entries from the documents
		// relation
		MapContract filterDocs = new MapContract(FilterDocs.class, docs, "Filter Docs");
		filterDocs.setDegreeOfParallelism(noSubTasks);
		filterDocs.getCompilerHints().setAvgRecordsEmittedPerStubCall(0.15f);
		filterDocs.getCompilerHints().setAvgBytesPerRecord(60);
		filterDocs.getCompilerHints().setAvgNumValuesPerDistinctValue(new FieldSet(new int[]{0}), 1);

		// Create MapContract for filtering the entries from the ranks relation
		MapContract filterRanks = new MapContract(FilterRanks.class, ranks, "Filter Ranks");
		filterRanks.setDegreeOfParallelism(noSubTasks);
		filterRanks.getCompilerHints().setAvgRecordsEmittedPerStubCall(0.25f);
		filterRanks.getCompilerHints().setAvgNumValuesPerDistinctValue(new FieldSet(new int[]{0}), 1);

		// Create MapContract for filtering the entries from the visits relation
		MapContract filterVisits = new MapContract(FilterVisits.class, visits, "Filter Visits");
		filterVisits.setDegreeOfParallelism(noSubTasks);
		filterVisits.getCompilerHints().setAvgBytesPerRecord(60);
		filterVisits.getCompilerHints().setAvgRecordsEmittedPerStubCall(0.2f);

		// Create MatchContract to join the filtered documents and ranks
		// relation
		MatchContract joinDocsRanks = new MatchContract(JoinDocRanks.class, PactString.class, 0, 0, filterDocs,
				filterRanks, "Join Docs Ranks");
		joinDocsRanks.setDegreeOfParallelism(noSubTasks);

		// Create CoGroupContract to realize a anti join between the joined
		// documents and ranks relation and the filtered visits relation
		CoGroupContract antiJoinVisits = new CoGroupContract(AntiJoinVisits.class, PactString.class, 0, 0, joinDocsRanks, 
				filterVisits, "Antijoin DocsVisits");
		antiJoinVisits.setDegreeOfParallelism(noSubTasks);
		antiJoinVisits.getCompilerHints().setAvgRecordsEmittedPerStubCall(0.8f);

		// Create DataSinkContract for writing the result of the OLAP query
		FileDataSink result = new FileDataSink(WebLogAnalysisOutFormat.class, output, antiJoinVisits, "Result");
		result.setDegreeOfParallelism(noSubTasks);

		// Return the PACT plan
		return new Plan(result, "Weblog Analysis");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDescription() {
		return "Parameters: [noSubTasks], [docs], [ranks], [visits], [output]";
	}

}
