/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.streaming.examples.iterative.pagerank;

import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.util.Collector;

public class PageRankSource implements SourceFunction<Tuple3<Integer, Integer, Long>> {
	private static final long serialVersionUID = 1L;

	private Tuple3<Integer, Integer, Long> outRecord = new Tuple3<Integer, Integer, Long>();
	private Long timestamp = 0L;

	@Override
	public void invoke(Collector<Tuple3<Integer, Integer, Long>> collector) throws Exception {
		@SuppressWarnings("resource")
		BufferedReader br = new BufferedReader(new FileReader(
				"src/test/resources/testdata/ASTopology.data"));
		while (true) {
			String line = br.readLine();
			if (line == null) {
				break;
			}
			if (!line.equals("")) {
				String[] link = line.split(":");
				outRecord.f0 = Integer.valueOf(link[0]);
				outRecord.f1 = Integer.valueOf(link[1]);
				outRecord.f2 = timestamp;
				collector.collect(outRecord);
				timestamp += 1;
			}
		}
	}
}
