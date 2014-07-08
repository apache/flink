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

import java.util.Iterator;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.CoGroupFunction;
import eu.stratosphere.api.java.functions.FilterFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.example.java.relational.util.WebLogData;
import eu.stratosphere.example.java.relational.util.WebLogDataGenerator;
import eu.stratosphere.util.Collector;

/**
 * This program processes web logs and relational data. 
 * It implements the following relational query:
 *
 * <code><pre>
 * SELECT 
 *       r.pageURL, 
 *       r.pageRank, 
 *       r.avgDuration
 * FROM documents d JOIN rankings r
 *                  ON d.url = r.url
 * WHERE CONTAINS(d.text, [keywords]) 
 *       AND r.rank > [rank] 
 *       AND NOT EXISTS 
 *           (
 *              SELECT * FROM Visits v
 *              WHERE v.destUrl = d.url 
 *                    AND v.visitDate < [date]
 *           );
 * </pre></code>
 *
 * <p>
 * Input files are plain text CSV files using the pipe character ('|') as field separator.
 * The tables referenced in the query can be generated using the {@link WebLogDataGenerator} and 
 * have the following schemas
 * <code><pre>
 * CREATE TABLE Documents (
 *                url VARCHAR(100) PRIMARY KEY,
 *                contents TEXT );
 *
 * CREATE TABLE Rankings (
 *                pageRank INT,
 *                pageURL VARCHAR(100) PRIMARY KEY,
 *                avgDuration INT );
 *
 * CREATE TABLE Visits (
 *                sourceIP VARCHAR(16),
 *                destURL VARCHAR(100),
 *                visitDate DATE,
 *                adRevenue FLOAT,
 *                userAgent VARCHAR(64),
 *                countryCode VARCHAR(3),
 *                languageCode VARCHAR(6),
 *                searchWord VARCHAR(32),
 *                duration INT );
 * </pre></code>
 * 
 * <p>
 * Usage: <code>WebLogAnalysis &lt;documents path&gt; &lt;ranks path&gt; &lt;visits path&gt; &lt;result path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from {@link WebLogData}.
 * 
 * <p>
 * This example shows how to use:
 * <ul>
 * <li> tuple data types
 * <li> projection and join projection
 * <li> the CoGroup transformation for an anti-join
 * </ul>
 * 
 */
@SuppressWarnings("serial")
public class WebLogAnalysis {
	
	// *************************************************************************
	//     PROGRAM
	// *************************************************************************
	
	public static void main(String[] args) throws Exception {
		
		if(!parseParameters(args)) {
			return;
		}

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataSet<Tuple2<String, String>> documents = getDocumentsDataSet(env);
		DataSet<Tuple3<Integer, String, Integer>> ranks = getRanksDataSet(env);
		DataSet<Tuple2<String, String>> visits = getVisitsDataSet(env);
		
		// Retain documents with keywords
		DataSet<Tuple1<String>> filterDocs = documents
				.filter(new FilterDocByKeyWords())
				.project(0).types(String.class);

		// Filter ranks by minimum rank
		DataSet<Tuple3<Integer, String, Integer>> filterRanks = ranks
				.filter(new FilterByRank());

		// Filter visits by visit date
		DataSet<Tuple1<String>> filterVisits = visits
				.filter(new FilterVisitsByDate())
				.project(0).types(String.class);

		// Join the filtered documents and ranks, i.e., get all URLs with min rank and keywords
		DataSet<Tuple3<Integer, String, Integer>> joinDocsRanks = 
				filterDocs.join(filterRanks)
							.where(0).equalTo(1)
							.projectSecond(0,1,2)
							.types(Integer.class, String.class, Integer.class);

		// Anti-join urls with visits, i.e., retain all URLs which have NOT been visited in a certain time
		DataSet<Tuple3<Integer, String, Integer>> result = 
				joinDocsRanks.coGroup(filterVisits)
								.where(1).equalTo(0)
								.with(new AntiJoinVisits());

		// emit result
		if(fileOutput) {
			result.writeAsCsv(outputPath, "\n", "|");
		} else {
			result.print();
		}

		// execute program
		env.execute("WebLogAnalysis Example");
		
	}
	
	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************
	
	/**
	 * MapFunction that filters for documents that contain a certain set of
	 * keywords.
	 */
	public static class FilterDocByKeyWords extends FilterFunction<Tuple2<String, String>> {

		private static final String[] KEYWORDS = { " editors ", " oscillations " };

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
	public static class FilterByRank extends FilterFunction<Tuple3<Integer, String, Integer>> {

		private static final int RANKFILTER = 40;

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
	public static class FilterVisitsByDate extends FilterFunction<Tuple2<String, String>> {

		private static final int YEARFILTER = 2007;

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

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************
	
	private static boolean fileOutput = false;
	private static String documentsPath;
	private static String ranksPath;
	private static String visitsPath;
	private static String outputPath;
	
	private static boolean parseParameters(String[] args) {
		
		if(args.length > 0) {
			fileOutput = true;
			if(args.length == 4) {
				documentsPath = args[0];
				ranksPath = args[1];
				visitsPath = args[2];
				outputPath = args[3];
			} else {
				System.err.println("Usage: WebLogAnalysis <documents path> <ranks path> <visits path> <result path>");
				return false;
			}
		} else {
			System.out.println("Executing WebLog Analysis example with built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  We provide a data generator to create synthetic input files for this program.");
			System.out.println("  Usage: WebLogAnalysis <documents path> <ranks path> <visits path> <result path>");
		}
		return true;
	}
	
	private static DataSet<Tuple2<String, String>> getDocumentsDataSet(ExecutionEnvironment env) {
		// Create DataSet for documents relation (URL, Doc-Text)
		if(fileOutput) {
			return env.readCsvFile(documentsPath)
						.fieldDelimiter('|')
						.types(String.class, String.class);
		} else {
			return WebLogData.getDocumentDataSet(env);
		}
	}
	
	private static DataSet<Tuple3<Integer, String, Integer>> getRanksDataSet(ExecutionEnvironment env) {
		// Create DataSet for ranks relation (Rank, URL, Avg-Visit-Duration)
		if(fileOutput) {
			return env.readCsvFile(ranksPath)
						.fieldDelimiter('|')
						.types(Integer.class, String.class, Integer.class);
		} else {
			return WebLogData.getRankDataSet(env);
		}
	}

	private static DataSet<Tuple2<String, String>> getVisitsDataSet(ExecutionEnvironment env) {
		// Create DataSet for visits relation (URL, Date)
		if(fileOutput) {
			return env.readCsvFile(visitsPath)
						.fieldDelimiter('|')
						.includeFields("011000000")
						.types(String.class, String.class);
		} else {
			return WebLogData.getVisitDataSet(env);
		}
	}
		
}
