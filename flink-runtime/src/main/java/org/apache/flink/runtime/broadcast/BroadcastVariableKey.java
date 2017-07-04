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

package org.apache.flink.runtime.broadcast;

import org.apache.flink.runtime.jobgraph.JobVertexID;

/**
 * An identifier for a {@link BroadcastVariableMaterialization} based on the task's {@link JobVertexID}, broadcast
 * variable name and iteration superstep.
 */
public class BroadcastVariableKey {

	private final JobVertexID vertexId;

	private final String name;

	private final int superstep;

	public BroadcastVariableKey(JobVertexID vertexId, String name, int superstep) {
		if (vertexId == null || name == null || superstep <= 0) {
			throw new IllegalArgumentException();
		}

		this.vertexId = vertexId;
		this.name = name;
		this.superstep = superstep;
	}

	// ---------------------------------------------------------------------------------------------

	public JobVertexID getVertexId() {
		return vertexId;
	}

	public String getName() {
		return name;
	}

	public int getSuperstep() {
		return superstep;
	}

	// ---------------------------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return 31 * superstep +
				47 * name.hashCode() +
				83 * vertexId.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj != null && obj.getClass() == BroadcastVariableKey.class) {
			BroadcastVariableKey other = (BroadcastVariableKey) obj;
			return this.superstep == other.superstep &&
					this.name.equals(other.name) &&
					this.vertexId.equals(other.vertexId);
		}
		else {
			return false;
		}
	}

	@Override
	public String toString() {
		return vertexId + " \"" + name + "\" (" + superstep + ')';
	}
}
