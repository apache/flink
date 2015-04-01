/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.recordJobs.relational;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.Program;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.record.functions.CoGroupFunction;
import org.apache.flink.api.java.record.functions.JoinFunction;
import org.apache.flink.api.java.record.functions.MapFunction;
import org.apache.flink.api.java.record.functions.FunctionAnnotation.ConstantFieldsExcept;
import org.apache.flink.api.java.record.functions.FunctionAnnotation.ConstantFieldsFirstExcept;
import org.apache.flink.api.java.record.functions.FunctionAnnotation.ConstantFieldsSecondExcept;
import org.apache.flink.api.java.record.io.CsvInputFormat;
import org.apache.flink.api.java.record.io.CsvOutputFormat;
import org.apache.flink.api.java.record.operators.CoGroupOperator;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.api.java.record.operators.JoinOperator;
import org.apache.flink.api.java.record.operators.MapOperator;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

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
 */
@SuppressWarnings("deprecation")
public class WebLogAnalysis implements Program, ProgramDescription {
	
	private static final long serialVersionUID = 1L;


	/**
	 * MapFunction that filters for documents that contain a certain set of
	 * keywords. 
	 */
	@ConstantFieldsExcept(1)
	public static class FilterDocs extends MapFunction implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private static final String[] KEYWORDS = { " editors ", " oscillations ", " convection " };
		
		/**
		 * Filters for documents that contain all of the given keywords and projects the records on the URL field.
		 * 
		 * Output Format:
		 * 0: URL
		 */
		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			// FILTER
			// Only collect the document if all keywords are contained
			String docText = record.getField(1, StringValue.class).toString();
			boolean allContained = true;
			for (String kw : KEYWORDS) {
				if (!docText.contains(kw)) {
					allContained = false;
					break;
				}
			}

			if (allContained) {
				record.setNull(1);
				out.collect(record);
			}
		}
	}

	/**
	 * MapFunction that filters for records where the rank exceeds a certain threshold.
	 */
	@ConstantFieldsExcept({})
	public static class FilterRanks extends MapFunction implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private static final int RANKFILTER = 50;
		
		/**
		 * Filters for records of the rank relation where the rank is greater
		 * than the given threshold.
		 * 
		 * Output Format:
		 * 0: URL
		 * 1: RANK
		 * 2: AVG_DURATION
		 */
		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			
			if (record.getField(1, IntValue.class).getValue() > RANKFILTER) {
				out.collect(record);
			}
		}
	}

	/**
	 * MapFunction that filters for records of the visits relation where the year
	 * (from the date string) is equal to a certain value.
	 */
	@ConstantFieldsExcept(1)
	public static class FilterVisits extends MapFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		private static final int YEARFILTER = 2010;
		
		/**
		 * Filters for records of the visits relation where the year of visit is equal to a
		 * specified value. The URL of all visit records passing the filter is emitted.
		 * 
		 * Output Format:
		 * 0: URL
		 */
		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			// Parse date string with the format YYYY-MM-DD and extract the year
			String dateString = record.getField(1, StringValue.class).getValue();
			int year = Integer.parseInt(dateString.substring(0,4)); 
			
			if (year == YEARFILTER) {
				record.setNull(1);
				out.collect(record);
				
			}
		}
	}

	/**
	 * JoinFunction that joins the filtered entries from the documents and the
	 * ranks relation.
	 */
	@ConstantFieldsSecondExcept({})
	public static class JoinDocRanks extends JoinFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		/**
		 * Joins entries from the documents and ranks relation on their URL.
		 * 
		 * Output Format:
		 * 0: URL
		 * 1: RANK
		 * 2: AVG_DURATION
		 */
		@Override
		public void join(Record document, Record rank, Collector<Record> out) throws Exception {
			out.collect(rank);	
		}
	}

	/**
	 * CoGroupFunction that realizes an anti-join.
	 * If the first input does not provide any pairs, all pairs of the second input are emitted.
	 * Otherwise, no pair is emitted.
	 */
	@ConstantFieldsFirstExcept({})
	public static class AntiJoinVisits extends CoGroupFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		/**
		 * If the visit iterator is empty, all pairs of the rank iterator are emitted.
		 * Otherwise, no pair is emitted. 
		 * 
		 * Output Format:
		 * 0: URL
		 * 1: RANK
		 * 2: AVG_DURATION
		 */
		@Override
		public void coGroup(Iterator<Record> ranks, Iterator<Record> visits, Collector<Record> out) {
			// Check if there is a entry in the visits relation
			if (!visits.hasNext()) {
				while (ranks.hasNext()) {
					// Emit all rank pairs
					out.collect(ranks.next());
				}
			}
		}
	}


	@Override
	public Plan getPlan(String... args) {

		// parse job parameters
		int numSubTasks     = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String docsInput   = (args.length > 1 ? args[1] : "");
		String ranksInput  = (args.length > 2 ? args[2] : "");
		String visitsInput = (args.length > 3 ? args[3] : "");
		String output      = (args.length > 4 ? args[4] : "");

		/*
		 * Output Format:
		 * 0: URL
		 * 1: DOCUMENT_TEXT
		 */
		// Create DataSourceContract for documents relation
		@SuppressWarnings("unchecked")
		CsvInputFormat docsFormat = new CsvInputFormat('|', StringValue.class, StringValue.class);
		FileDataSource docs = new FileDataSource(docsFormat, docsInput, "Docs Input");
		
		/*
		 * Output Format:
		 * 0: URL
		 * 1: RANK
		 * 2: AVG_DURATION
		 */
		// Create DataSourceContract for ranks relation
		FileDataSource ranks = new FileDataSource(new CsvInputFormat(), ranksInput, "Ranks input");
		CsvInputFormat.configureRecordFormat(ranks)
			.recordDelimiter('\n')
			.fieldDelimiter('|')
			.field(StringValue.class, 1)
			.field(IntValue.class, 0)
			.field(IntValue.class, 2);

		/*
		 * Output Format:
		 * 0: URL
		 * 1: DATE
		 */
		// Create DataSourceContract for visits relation
		@SuppressWarnings("unchecked")
		CsvInputFormat visitsFormat = new CsvInputFormat('|', null, StringValue.class, StringValue.class);
		FileDataSource visits = new FileDataSource(visitsFormat, visitsInput, "Visits input:q");

		// Create MapOperator for filtering the entries from the documents
		// relation
		MapOperator filterDocs = MapOperator.builder(new FilterDocs())
			.input(docs)
			.name("Filter Docs")
			.build();
		filterDocs.getCompilerHints().setFilterFactor(0.15f);

		// Create MapOperator for filtering the entries from the ranks relation
		MapOperator filterRanks = MapOperator.builder(new FilterRanks())
			.input(ranks)
			.name("Filter Ranks")
			.build();
		filterRanks.getCompilerHints().setFilterFactor(0.25f);

		// Create MapOperator for filtering the entries from the visits relation
		MapOperator filterVisits = MapOperator.builder(new FilterVisits())
			.input(visits)
			.name("Filter Visits")
			.build();
		filterVisits.getCompilerHints().setFilterFactor(0.2f);

		// Create JoinOperator to join the filtered documents and ranks
		// relation
		JoinOperator joinDocsRanks = JoinOperator.builder(new JoinDocRanks(), StringValue.class, 0, 0)
			.input1(filterDocs)
			.input2(filterRanks)
			.name("Join Docs Ranks")
			.build();

		// Create CoGroupOperator to realize a anti join between the joined
		// documents and ranks relation and the filtered visits relation
		CoGroupOperator antiJoinVisits = CoGroupOperator.builder(new AntiJoinVisits(), StringValue.class, 0, 0)
			.input1(joinDocsRanks)
			.input2(filterVisits)
			.name("Antijoin DocsVisits")
			.build();

		// Create DataSinkContract for writing the result of the OLAP query
		FileDataSink result = new FileDataSink(new CsvOutputFormat(), output, antiJoinVisits, "Result");
		result.setParallelism(numSubTasks);
		CsvOutputFormat.configureRecordFormat(result)
			.recordDelimiter('\n')
			.fieldDelimiter('|')
			.lenient(true)
			.field(IntValue.class, 1)
			.field(StringValue.class, 0)
			.field(IntValue.class, 2);

		// Return the PACT plan
		Plan p = new Plan(result, "Weblog Analysis");
		p.setDefaultParallelism(numSubTasks);
		return p;
	}


	@Override
	public String getDescription() {
		return "Parameters: [numSubTasks], [docs], [ranks], [visits], [output]";
	}
}
