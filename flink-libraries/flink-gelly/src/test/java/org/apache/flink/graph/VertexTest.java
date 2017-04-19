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

import org.apache.flink.types.NullValue;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VertexTest {

	@Test
	public void testCreateVertexWithNoValue() {
		Vertex<Integer, NullValue> vertex = Vertex.create(1);
		assertEquals(1, (long) vertex.getId());
		assertEquals(NullValue.getInstance(), vertex.getValue());
	}

	@Test
	public void testCreateVertexWithValue() {
		Vertex<Integer, Integer> vertex = Vertex.create(1, 2);
		assertEquals(1, (long) vertex.getId());
		assertEquals(2, (long) vertex.getValue());
	}
}
