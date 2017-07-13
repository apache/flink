/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.graph.asm.result;

/**
 * Base class for algorithm results for three vertices.
 *
 * @param <K> graph ID type
 */
public abstract class TertiaryResultBase<K>
extends ResultBase
implements TertiaryResult<K> {

	private K vertexId0;

	private K vertexId1;

	private K vertexId2;

	@Override
	public K getVertexId0() {
		return vertexId0;
	}

	@Override
	public void setVertexId0(K vertexId0) {
		this.vertexId0 = vertexId0;
	}

	@Override
	public K getVertexId1() {
		return vertexId1;
	}

	@Override
	public void setVertexId1(K vertexId1) {
		this.vertexId1 = vertexId1;
	}

	@Override
	public K getVertexId2() {
		return vertexId2;
	}

	@Override
	public void setVertexId2(K vertexId2) {
		this.vertexId2 = vertexId2;
	}
}
