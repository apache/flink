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

import java.io.Serializable;

/**
 * Represents the graph's nodes. It carries an ID and a value as well as the vertex inDegree and outDegree.
 * For vertices with no value, use {@link org.apache.flink.types.NullValue} as the value type.
 *
 * @param <K>
 * @param <V>
 */
public class VertexWithDegrees<K extends Comparable<K> & Serializable, V extends Serializable>
		extends Vertex<K, V> {

	private long inDegree;

	private long outDegree;

	public VertexWithDegrees() {
		super();
		inDegree = -1l;
		outDegree = -1l;
	}

	public VertexWithDegrees(K k, V v) {
		super(k,v);
		inDegree = 0l;
		outDegree = 0l;
	}

	public Long getInDegree() throws Exception{
		if(inDegree == -1) {
			throw new InaccessibleMethodException("The degree option was not set. To access the degrees, " +
					"call iterationConfiguration.setOptDegrees(true).");
		}
		return inDegree;
	}

	public void setInDegree(Long inDegree) {
		this.inDegree = inDegree;
	}

	public Long getOutDegree() throws Exception{
		if(outDegree == -1) {
			throw new InaccessibleMethodException("The degree option was not set. To access the degrees, " +
					"call iterationConfiguration.setOptDegrees(true).");
		}
		return outDegree;
	}

	public void setOutDegree(Long outDegree) {
		this.outDegree = outDegree;
	}
}
