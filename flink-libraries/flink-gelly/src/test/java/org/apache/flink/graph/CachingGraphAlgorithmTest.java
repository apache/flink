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

package org.apache.flink.graph;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.generator.EmptyGraph;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class CachingGraphAlgorithmTest {

	private ExecutionEnvironment env;

	@Before
	public void setup() {
		env = ExecutionEnvironment.createCollectionsEnvironment();
	}

	@Test
	public void testSameOutput()
			throws Exception {
		Graph<LongValue,NullValue,NullValue> graph = new EmptyGraph(env, 32L)
			.generate();

		GraphAlgorithm<LongValue,NullValue,NullValue,DataSet<LongValue>> algorithm0 =
			new SimpleGraphAlgorithm(13, 42L, "Good day, Flink!");
		DataSet<LongValue> output0 = algorithm0.run(graph);

		GraphAlgorithm<LongValue,NullValue,NullValue,DataSet<LongValue>> algorithm1 =
			new SimpleGraphAlgorithm(13, 42L, "Good night, Flink!");
		DataSet<LongValue> output1 = algorithm1.run(graph);

		// SimpleGraphAlgorithm equality is dependent only on the first two parameters;
		// the algorithms are equal so the algorithm output objects are the same
		assertTrue(output0 == output1);

		GraphAlgorithm<LongValue,NullValue,NullValue,DataSet<LongValue>> algorithm2 =
			new SimpleGraphAlgorithm(13, 97L, "Good day, Flink!");
		DataSet<LongValue> output2 = algorithm2.run(graph);

		// different configuration yields different output objects
		assertTrue(output0 != output2);
	}

	private static class SimpleGraphAlgorithm
	extends CachingGraphAlgorithm<LongValue, NullValue, NullValue, DataSet<LongValue>> {
		private final int field0;

		private final long field1;

		private final String field2;

		public SimpleGraphAlgorithm(int field0, long field1, String field2) {
			this.field0 = field0;
			this.field1 = field1;
			this.field2 = field2;
		}

		@Override
		protected void hashCodeInternal(HashCodeBuilder builder) {
			builder
				.append(field0)
				.append(field1);
		}

		@Override
		protected void equalsInternal(EqualsBuilder builder, CachingGraphAlgorithm obj) {
			if (! SimpleGraphAlgorithm.class.isAssignableFrom(obj.getClass())) {
				builder.appendSuper(false);
			}

			SimpleGraphAlgorithm rhs = (SimpleGraphAlgorithm) obj;

			builder
				.append(field0, rhs.field0)
				.append(field1, rhs.field1);
		}

		@Override
		protected String getAlgorithmName() {
			return SimpleGraphAlgorithm.class.getCanonicalName();
		}

		@Override
		protected DataSet<LongValue> runInternal(Graph<LongValue,NullValue,NullValue> input)
				throws Exception {
			return input.getVertexIds();
		}
	}
}
