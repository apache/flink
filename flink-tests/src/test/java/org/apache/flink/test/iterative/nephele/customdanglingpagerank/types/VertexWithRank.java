/**
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

package org.apache.flink.test.iterative.nephele.customdanglingpagerank.types;

import java.io.Serializable;


/**
 *
 */
public final class VertexWithRank implements Serializable {
	private static final long serialVersionUID = 1L;

	private long vertexID;
	
	private double rank;

	
	public VertexWithRank() {
	}
	
	public VertexWithRank(long vertexID, double rank) {
		this.vertexID = vertexID;
		this.rank = rank;
	}

	
	public long getVertexID() {
		return vertexID;
	}
	
	public void setVertexID(long vertexID) {
		this.vertexID = vertexID;
	}
	
	public double getRank() {
		return rank;
	}
	
	public void setRank(double rank) {
		this.rank = rank;
	}
	

	@Override
	public String toString() {
		return this.vertexID + " - " + this.rank;
	}
}
