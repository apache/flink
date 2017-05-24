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

package org.apache.flink.graph.gsa;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * This class represents a {@code <sourceVertex, edge>} pair. This is a wrapper
 * around {@code Tuple2<VV, EV>} for convenience in the GatherFunction.
 *
 * @param <VV> the vertex value type
 * @param <EV> the edge value type
 */
@SuppressWarnings("serial")
public class Neighbor<VV, EV> extends Tuple2<VV, EV> {

	public Neighbor() {}

	public Neighbor(VV neighborValue, EV edgeValue) {
		super(neighborValue, edgeValue);
	}

	public VV getNeighborValue() {
		return this.f0;
	}

	public EV getEdgeValue() {
		return this.f1;
	}
}
