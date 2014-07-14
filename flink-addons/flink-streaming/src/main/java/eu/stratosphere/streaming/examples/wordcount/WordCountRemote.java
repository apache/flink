/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.streaming.examples.wordcount;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;

import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.client.program.Client;
import eu.stratosphere.client.program.JobWithJars;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.streaming.api.JobGraphBuilder;
import eu.stratosphere.streaming.api.invokable.UserSinkInvokable;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.util.LogUtils;

public class WordCountRemote {
	private final static int recordsEmitted = 100000;
	private final static int statPerRecords = 10000;
	private final static int recordsEmittedFromSplitter = 350000; // recordsEmitted * wordsEmittedInARecord
	private final static int statPerRecordsAfterSplit = recordsEmittedFromSplitter / recordsEmitted * statPerRecords;

	public static class WordCountDebugSource extends UserSourceInvokable {
		int statCounter = 0;
		int[] atNumOfRecords = new int[recordsEmitted / statPerRecords + 1];
		long[] times = new long[recordsEmitted / statPerRecords + 1];

		StreamRecord record = new StreamRecord(new Tuple1<String>());

		@Override
		public void invoke() throws Exception {
			atNumOfRecords[statCounter] = 0;
			times[statCounter] = System.currentTimeMillis();
			statCounter++;

			for (int i = 1; i <= recordsEmitted; i++) {
				if (i % statPerRecords == 0) {
					atNumOfRecords[statCounter] = i;
					times[statCounter] = System.currentTimeMillis();
					statCounter++;
				}

				if (i % 2 == 0) {
					record.setString(0, "Gyula Marci switched");
				} else {
					record.setString(0, "Gabor Frank to FINISHED");
				}
				emit(record);
			}
		}

		@Override
		public String getResult() {
			StringBuilder result = new StringBuilder("");
			result.append("Source result:\n");
			for (int i = 0; i < atNumOfRecords.length; i++) {
				result.append(atNumOfRecords[i] + ";" + times[i] + ";\n");
			}
			return result.toString();
		}
	}

	public static class WordCountDebugSplitter extends UserTaskInvokable {
		int statCounter = 0;
		int[] atNumOfRecords = new int[recordsEmitted / statPerRecords + 1];
		long[] times = new long[atNumOfRecords.length];

		private String[] words = new String[] {};
		private int i = 0;
		private StreamRecord outputRecord = new StreamRecord(new Tuple1<String>());
		long time = System.currentTimeMillis();

		
		@Override
		public void invoke(StreamRecord record) throws Exception {
			i++;
			if (i % statPerRecords == 0) {
				atNumOfRecords[statCounter] = i;
				times[statCounter] = System.currentTimeMillis();
				statCounter++;
			}

			words = record.getString(0).split(" ");
			for (String word : words) {
				outputRecord.setString(0, word);
				emit(outputRecord);
			}
		}

		@Override
		public String getResult() {
			StringBuilder result = new StringBuilder("");
			result.append("Splitter result:\n");
			for (int i = 0; i < atNumOfRecords.length; i++) {
				result.append(atNumOfRecords[i] + ";" + times[i] + ";\n");
			}
			return result.toString();
		}
	}

	public static class WordCountDebugCounter extends UserTaskInvokable {
		int statCounter = 0;
		int[] atNumOfRecords = new int[recordsEmitted / statPerRecords + 1];
		long[] times = new long[atNumOfRecords.length];
		
		private Map<String, Integer> wordCounts = new HashMap<String, Integer>();
		private String word = "";
		private Integer count = 0;

		private int i = 0;

		private StreamRecord outRecord = new StreamRecord(new Tuple2<String, Integer>());

		@Override
		public void invoke(StreamRecord record) throws Exception {
			word = record.getString(0);
			i++;
			
			if (i % statPerRecordsAfterSplit == 0) {
				atNumOfRecords[statCounter] = i;
				times[statCounter] = System.currentTimeMillis();
				statCounter++;
			}
			
			if (wordCounts.containsKey(word)) {
				count = wordCounts.get(word) + 1;
				wordCounts.put(word, count);
			} else {
				count = 1;
				wordCounts.put(word, 1);
			}

			outRecord.setString(0, word);
			outRecord.setInteger(1, count);

			emit(outRecord);
		}
		
		@Override
		public String getResult() {
			StringBuilder result = new StringBuilder("");
			result.append("Counter result:\n");
			for (int i = 0; i < atNumOfRecords.length; i++) {
				result.append(atNumOfRecords[i] + ";" + times[i] + ";\n");
			}
			return result.toString();
		}
	}

	public static class WordCountDebugSink extends UserSinkInvokable {
		int nrOfRecords = 0;
		int[] atNumOfRecords = new int[recordsEmitted / statPerRecords + 1];
		long[] times = new long[atNumOfRecords.length];
		
		@Override
		public void invoke(StreamRecord record) throws Exception {
			if (nrOfRecords % statPerRecordsAfterSplit == 0) {
				atNumOfRecords[nrOfRecords] = nrOfRecords;
				times[nrOfRecords] = System.currentTimeMillis();
				nrOfRecords++;
			}
		}

		@Override
		public String getResult() {
			StringBuilder result = new StringBuilder("");
			result.append("Sink result:\n");
			for (int i = 0; i < atNumOfRecords.length; i++) {
				result.append(atNumOfRecords[i] + ";" + times[i] + ";\n");
			}
			result.append("RESULT: " + nrOfRecords);
			return result.toString();
		}
	}

	private static JobGraph getJobGraph() throws Exception {
		JobGraphBuilder graphBuilder = new JobGraphBuilder("testGraph");
		graphBuilder.setSource("WordCountSource", WordCountDebugSource.class, 2, 1);
		graphBuilder.setTask("WordCountSplitter", WordCountDebugSplitter.class, 2, 1);
		graphBuilder.setTask("WordCountCounter", WordCountDebugCounter.class, 2, 1);
		graphBuilder.setSink("WordCountSink", WordCountDebugSink.class);

		graphBuilder.shuffleConnect("WordCountSource", "WordCountSplitter");
		graphBuilder.fieldsConnect("WordCountSplitter", "WordCountCounter", 0);
		graphBuilder.shuffleConnect("WordCountCounter", "WordCountSink");

		return graphBuilder.getJobGraph();
	}

	public static void main(String[] args) {
		LogUtils.initializeDefaultConsoleLogger(Level.DEBUG, Level.INFO);

		try {
			File file = new File("target/stratosphere-streaming-0.5-SNAPSHOT.jar");
			JobWithJars.checkJarFile(file);

			JobGraph jG = getJobGraph();

			jG.addJar(new Path(file.getAbsolutePath()));

			Configuration configuration = jG.getJobConfiguration();
			Client client = new Client(new InetSocketAddress("hadoop00.ilab.sztaki.hu", 6123), configuration);
			client.run(jG, true);
		} catch (Exception e) {
			System.out.println(e);
		}

	}
}
