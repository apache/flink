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

package org.apache.flink.test.runtime;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.junit.Rule;
import org.junit.rules.Timeout;

import java.io.IOException;

/**
 * Tests a join, which leads to a deadlock with large data sizes and PIPELINED-only execution.
 *
 * @see <a href="https://issues.apache.org/jira/browse/FLINK-1343">FLINK-1343</a>
 */
public class JoinDeadlockITCase extends JavaProgramTestBase {

	protected String resultPath;

	@Rule
	public Timeout globalTimeout = new Timeout(120 * 1000); // Set timeout for deadlocks

	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void testProgram() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Long> longs = env.createInput(new DataGeneratorInputFormat(1000000));

		DataSet<Tuple1<Long>> longT1 = longs.map(new TupleWrapper());
		DataSet<Tuple1<Long>> longT2 = longT1.project(0);
		DataSet<Tuple1<Long>> longT3 = longs.map(new TupleWrapper());

		longT2.join(longT3).where(0).equalTo(0).projectFirst(0)
				.join(longT1).where(0).equalTo(0).projectFirst(0)
				.writeAsText(resultPath);

		env.execute();
	}

	public static class TupleWrapper implements MapFunction<Long, Tuple1<Long>> {

		@Override
		public Tuple1<Long> map(Long l) throws Exception {
			return new Tuple1<Long>(l);
		}
	}

	// ------------------------------------------------------------------------

	// Use custom input format to generate the data. Other available input formats (like collection
	// input format) create data upfront and serialize it completely on the heap, which might
	// break the test JVM heap sizes.
	private static class DataGeneratorInputFormat extends GenericInputFormat<Long> implements NonParallelInput {

		private static final long serialVersionUID = 1L;

		private final int end;

		private int current;

		public DataGeneratorInputFormat(int end) {
			this.end = end;
		}

		@Override
		public boolean reachedEnd() throws IOException {
			return current > end;
		}

		@Override
		public Long nextRecord(Long reuse) throws IOException {
			return new Long(current++);
		}
	}
}
