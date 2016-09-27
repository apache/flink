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

import org.apache.flink.api.java.tuple.Tuple3;

/**
 *
 * A BipartiteEdge represents a link between a top and bottom vertices
 * in a {@link BipartiteGraph}. It is similar to the {@link Edge} class
 * with the only difference that key of connected vertices can have
 * different types.
 *
 * @param <KT> the key type of the top vertices
 * @param <KB> the key type of the bottom vertices
 * @param <V> the edge value type
 */
public class BipartiteEdge<KT, KB, V> extends Tuple3<KT, KB, V> {

	private static final long serialVersionUID = 1L;

	public BipartiteEdge(){}

	public BipartiteEdge(KT topId, KB bottomId, V value) {
		this.f0 = topId;
		this.f1 = bottomId;
		this.f2 = value;
	}

	public KT getTopId() {
		return this.f0;
	}

	public void setTopId(KT i) {
		this.f0 = i;
	}

	public KB getBottomId() {
		return this.f1;
	}

	public void setBottomId(KB i) {
		this.f1 = i;
	}

	public V getValue() {
		return this.f2;
	}

	public void setValue(V newVal) {
		this.f2 = newVal;
	}
}
