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

package org.apache.flink.test.iterative;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.test.util.JavaProgramTestBase;

@SuppressWarnings("serial")
public class EmptyWorksetIterationITCase extends JavaProgramTestBase {
	
	private List<Tuple2<Long, Long>> result = new ArrayList<Tuple2<Long, Long>>();
	
	@Override
	protected void testProgram() throws Exception {
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Tuple2<Long, Long>> input = env.generateSequence(1, 20).map(new Dupl());
				
		DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iter = input.iterateDelta(input, 20, 0);
		iter.closeWith(iter.getWorkset(), iter.getWorkset())
			.output(new LocalCollectionOutputFormat<Tuple2<Long, Long>>(result));
		
		env.execute();
	}

	public static final class Dupl implements MapFunction<Long, Tuple2<Long, Long>> {

		@Override
		public Tuple2<Long, Long> map(Long value) {
			return new Tuple2<Long, Long>(value, value);
		}
		
	}
}
