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

package org.apache.flink.streaming.test.exampleJavaPrograms.join;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.join.WindowJoin;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;

import org.junit.Test;

import java.io.File;

@SuppressWarnings("serial")
public class WindowJoinITCase extends StreamingMultipleProgramsTestBase {

	@Test
	public void testProgram() throws Exception {
		final String resultPath = File.createTempFile("result-path", "dir").toURI().toString();
		try {
			final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
			
			DataStream<Tuple2<String, Integer>> grades = env
					.fromElements(WindowJoinData.GRADES_INPUT.split("\n"))
					.map(new Parser());
	
			DataStream<Tuple2<String, Integer>> salaries = env
					.fromElements(WindowJoinData.SALARIES_INPUT.split("\n"))
					.map(new Parser());
			
			WindowJoin
					.runWindowJoin(grades, salaries, 100)
					.writeAsText(resultPath, WriteMode.OVERWRITE);
			
			env.execute();

			// since the two sides of the join might have different speed
			// the exact output can not be checked just whether it is well-formed
			// checks that the result lines look like e.g. (bob, 2, 2015)
			checkLinesAgainstRegexp(resultPath, "^\\([a-z]+,(\\d),(\\d)+\\)");
		}
		finally {
			try {
				FileUtils.deleteDirectory(new File(resultPath));
			} catch (Throwable ignored) {}
		}
	}
	
	//-------------------------------------------------------------------------
	
	public static final class Parser implements MapFunction<String, Tuple2<String, Integer>> {

		@Override
		public Tuple2<String, Integer> map(String value) throws Exception {
			String[] fields = value.split(",");
			return new Tuple2<>(fields[1], Integer.parseInt(fields[2]));
		}
	}
}
