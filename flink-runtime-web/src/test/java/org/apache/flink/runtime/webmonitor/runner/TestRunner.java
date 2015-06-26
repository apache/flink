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

package org.apache.flink.runtime.webmonitor.runner;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.examples.java.relational.util.WebLogData;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.util.Collector;

/**
 * Simple runner that brings up a local cluster with the web server and executes two
 * jobs to expose their data in the archive
 */
@SuppressWarnings("serial")
public class TestRunner {

	public static void main(String[] args) throws Exception {

		// start the cluster with the runtime monitor
		Configuration configuration = new Configuration();
		configuration.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
		configuration.setBoolean(ConfigConstants.JOB_MANAGER_NEW_WEB_FRONTEND_KEY, true);
		configuration.setString(ConfigConstants.JOB_MANAGER_WEB_DOC_ROOT_KEY,
			"flink-dist/target/flink-0.10-SNAPSHOT-bin/flink-0.10-SNAPSHOT/resources/web-runtime-monitor");
		
		LocalFlinkMiniCluster cluster = new LocalFlinkMiniCluster(configuration, false);
		cluster.start();

		final int port = cluster.getLeaderRPCPort();
		runWordCount(port);
		runWebLogAnalysisExample(port);
		runWordCount(port);

		// block the thread
		Object o = new Object();
		synchronized (o) {
			o.wait();
		}
		
		cluster.shutdown();
	}
	
	private static void runWordCount(int port) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("localhost", port);
		
		DataSet<String> text = env.fromElements(WordCountData.TEXT.split("\n"));

		DataSet<Tuple2<String, Integer>> counts =
				// split up the lines in pairs (2-tuples) containing: (word,1)
				text.flatMap(new Tokenizer())
						// group by the tuple field "0" and sum up tuple field "1"
						.groupBy(0)
						.sum(1);

		counts.print();
	}
	
	private static void runWebLogAnalysisExample(int port) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("localhost", port);

		// get input data
		DataSet<Tuple2<String, String>> documents = WebLogData.getDocumentDataSet(env);
		DataSet<Tuple3<Integer, String, Integer>> ranks = WebLogData.getRankDataSet(env);
		DataSet<Tuple2<String, String>> visits = WebLogData.getVisitDataSet(env);

		// Retain documents with keywords
		DataSet<Tuple1<String>> filterDocs = documents
				.filter(new FilterDocByKeyWords())
				.project(0);

		// Filter ranks by minimum rank
		DataSet<Tuple3<Integer, String, Integer>> filterRanks = ranks
				.filter(new FilterByRank());

		// Filter visits by visit date
		DataSet<Tuple1<String>> filterVisits = visits
				.filter(new FilterVisitsByDate())
				.project(0);

		// Join the filtered documents and ranks, i.e., get all URLs with min rank and keywords
		DataSet<Tuple3<Integer, String, Integer>> joinDocsRanks =
				filterDocs.join(filterRanks)
						.where(0).equalTo(1)
						.projectSecond(0,1,2);

		// Anti-join urls with visits, i.e., retain all URLs which have NOT been visited in a certain time
		DataSet<Tuple3<Integer, String, Integer>> result =
				joinDocsRanks.coGroup(filterVisits)
						.where(1).equalTo(0)
						.with(new AntiJoinVisits());

		result.print();
	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}
	
	public static class FilterDocByKeyWords implements FilterFunction<Tuple2<String, String>> {

		private static final String[] KEYWORDS = { " editors ", " oscillations " };

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

	public static class FilterByRank implements FilterFunction<Tuple3<Integer, String, Integer>> {

		private static final int RANKFILTER = 40;

		@Override
		public boolean filter(Tuple3<Integer, String, Integer> value) throws Exception {
			return (value.f0 > RANKFILTER);
		}
	}


	public static class FilterVisitsByDate implements FilterFunction<Tuple2<String, String>> {

		private static final int YEARFILTER = 2007;

		@Override
		public boolean filter(Tuple2<String, String> value) throws Exception {
			// Parse date string with the format YYYY-MM-DD and extract the year
			String dateString = value.f1;
			int year = Integer.parseInt(dateString.substring(0,4));
			return (year == YEARFILTER);
		}
	}
	
	
	@FunctionAnnotation.ForwardedFieldsFirst("*")
	public static class AntiJoinVisits implements CoGroupFunction<Tuple3<Integer, String, Integer>, Tuple1<String>, Tuple3<Integer, String, Integer>> {

		@Override
		public void coGroup(Iterable<Tuple3<Integer, String, Integer>> ranks, Iterable<Tuple1<String>> visits, Collector<Tuple3<Integer, String, Integer>> out) {
			// Check if there is a entry in the visits relation
			if (!visits.iterator().hasNext()) {
				for (Tuple3<Integer, String, Integer> next : ranks) {
					// Emit all rank pairs
					out.collect(next);
				}
			}
		}
	}
}
