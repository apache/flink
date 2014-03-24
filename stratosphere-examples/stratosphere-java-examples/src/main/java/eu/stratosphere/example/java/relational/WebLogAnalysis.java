/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.example.java.relational;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.CoGroupFunction;
import eu.stratosphere.api.java.functions.FilterFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.util.Collector;

import java.util.Iterator;

/**
 * Implements the following relational OLAP query as Stratosphere program:
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
 */
@SuppressWarnings("serial")
public class WebLogAnalysis {

	/**
	 * MapFunction that filters for documents that contain a certain set of
	 * keywords.
	 */
	public static class FilterDocs extends FilterFunction<Tuple2<String, String>> {

		private static final String[] KEYWORDS = { " editors ", " oscillations ", " convection " };

		/**
		 * Filters for documents that contain all of the given keywords and projects the records on the URL field.
		 *
		 * Output Format:
		 * 0: URL
		 * 1: DOCUMENT_TEXT
		 */
		@Override
		public boolean filter(Tuple2<String, String> value) throws Exception {
			// FILTER
			// Only collect the document if all keywords are contained
			String docText = value.f1;
			for (String kw : KEYWORDS) {
				if (!docText.contains(kw)) {
					return false;
				}
			}
			return true;
		}
	}

	/**
	 * MapFunction that filters for records where the rank exceeds a certain threshold.
	 */
	public static class FilterRanks extends FilterFunction<Tuple3<Integer, String, Integer>> {

		private static final int RANKFILTER = 50;

		/**
		 * Filters for records of the rank relation where the rank is greater
		 * than the given threshold.
		 *
		 * Output Format:
		 * 0: RANK
		 * 1: URL
		 * 2: AVG_DURATION
		 */
		@Override
		public boolean filter(Tuple3<Integer, String, Integer> value) throws Exception {
			return (value.f0 > RANKFILTER);
		}
	}

	/**
	 * MapFunction that filters for records of the visits relation where the year
	 * (from the date string) is equal to a certain value.
	 */
	public static class FilterVisits extends FilterFunction<Tuple2<String, String>> {

		private static final int YEARFILTER = 2010;

		/**
		 * Filters for records of the visits relation where the year of visit is equal to a
		 * specified value. The URL of all visit records passing the filter is emitted.
		 *
		 * Output Format:
		 * 0: URL
		 * 1: DATE
		 */
		@Override
		public boolean filter(Tuple2<String, String> value) throws Exception {
			// Parse date string with the format YYYY-MM-DD and extract the year
			String dateString = value.f1;
			int year = Integer.parseInt(dateString.substring(0,4));
			return (year == YEARFILTER);
		}
	}


	/**
	 * CoGroupFunction that realizes an anti-join.
	 * If the first input does not provide any pairs, all pairs of the second input are emitted.
	 * Otherwise, no pair is emitted.
	 */
	public static class AntiJoinVisits extends CoGroupFunction<Tuple3<Integer, String, Integer>, Tuple1<String>, Tuple3<Integer, String, Integer>> {

		/**
		 * If the visit iterator is empty, all pairs of the rank iterator are emitted.
		 * Otherwise, no pair is emitted.
		 *
		 * Output Format:
		 * 0: RANK
		 * 1: URL
		 * 2: AVG_DURATION
		 */
		@Override
		public void coGroup(Iterator<Tuple3<Integer, String, Integer>> ranks, Iterator<Tuple1<String>> visits, Collector<Tuple3<Integer, String, Integer>> out) {
			// Check if there is a entry in the visits relation
			if (!visits.hasNext()) {
				while (ranks.hasNext()) {
					// Emit all rank pairs
					out.collect(ranks.next());
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.err.println("Parameters: <docs> <ranks> <visits> <output>.");
			System.err.println("            If <output> is \"STDOUT\", prints result to the command line.");
			return;
		}
		
		String docsInput   = args[0];
		String ranksInput  = args[1];
		String visitsInput = args[2];
		String output      = args[3];

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		/*
		 * Output Format:
		 * 0: URL
		 * 1: DOCUMENT_TEXT
		 */
		// Create DataSet for documents relation
		DataSet<Tuple2<String, String>> docs = env.readCsvFile(docsInput)
			.fieldDelimiter('|')
			.types(String.class, String.class);

		/*
		 * Output Format:
		 * 0: RANK
		 * 1: URL
		 * 2: AVG_DURATION
		 */
		// Create DataSet for ranks relation
		DataSet<Tuple3<Integer, String, Integer>> ranks = env.readCsvFile(ranksInput)
			.fieldDelimiter('|')
			.types(Integer.class, String.class, Integer.class);

		/*
		 * Output Format:
		 * 0: URL
		 * 1: DATE
		 */
		// Create DataSet for visits relation
		DataSet<Tuple2<String, String>> visits = env.readCsvFile(visitsInput)
			.fieldDelimiter('|')
			.includeFields("011000000")
			.types(String.class, String.class);

		// Create DataSet for filtering the entries from the documents relation
		DataSet<Tuple1<String>> filterDocs = docs.filter(new FilterDocs()).project(0).types(String.class);

		// Create DataSet for filtering the entries from the ranks relation
		DataSet<Tuple3<Integer, String, Integer>> filterRanks = ranks.filter(new FilterRanks());

		// Create DataSet for filtering the entries from the visits relation
		DataSet<Tuple1<String>> filterVisits = visits.filter(new FilterVisits()).project(0).types(String.class);

		// Create DataSet to join the filtered documents and ranks relation
		DataSet<Tuple3<Integer, String, Integer>> joinDocsRanks = filterDocs.join(filterRanks)
			.where(0).equalTo(1).projectSecond(0,1,2).types(Integer.class, String.class, Integer.class);

		// Create DataSet to realize a anti join between the joined
		// documents and ranks relation and the filtered visits relation
		DataSet<Tuple3<Integer, String, Integer>> antiJoinVisits = joinDocsRanks.coGroup(filterVisits)
			.where(1).equalTo(0).with(new AntiJoinVisits());

		if (output.equals("STDOUT")) {
			antiJoinVisits.print();
		} else {
			antiJoinVisits.writeAsCsv(output, "\n", "|");
		}

		env.execute();
	}
}
