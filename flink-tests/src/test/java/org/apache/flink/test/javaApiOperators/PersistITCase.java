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

package org.apache.flink.test.javaApiOperators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class PersistITCase extends MultipleProgramsTestBase {

	public PersistITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Test
	public void testPersist() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Long> data = env.generateSequence(1, 100).map(new MapFunction<Long, Long>() {
			@Override
			public Long map(Long value) throws Exception {
				return new Random().nextLong();
			}
		}).persist();

		// if the input was indeed persisted, the result on all collects will be the same.
		List<Long> exec1 = data.collect();
		List<Long> exec2 = data.collect();
		Collections.sort(exec1);
		Collections.sort(exec2);
		assertEquals(exec1, exec2);
	}
}
