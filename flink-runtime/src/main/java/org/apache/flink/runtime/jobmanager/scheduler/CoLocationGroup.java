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

package org.apache.flink.runtime.jobmanager.scheduler;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.util.AbstractID;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;

import com.google.common.base.Preconditions;

public class CoLocationGroup implements java.io.Serializable {
	
	private static final long serialVersionUID = -2605819490401895297L;

	// we use a job vertex ID, because the co location group acts as a unit inside which exclusive sharing of
	// slots is used
	private final AbstractID id = new AbstractID();
	
	private final List<AbstractJobVertex> vertices = new ArrayList<AbstractJobVertex>();
	
	private transient ArrayList<CoLocationConstraint> constraints;
	
	// --------------------------------------------------------------------------------------------
	
	public CoLocationGroup() {}
	
	public CoLocationGroup(AbstractJobVertex... vertices) {
		for (AbstractJobVertex v : vertices) {
			this.vertices.add(v);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	public void addVertex(AbstractJobVertex vertex) {
		Preconditions.checkNotNull(vertex);
		this.vertices.add(vertex);
	}
	
	public void mergeInto(CoLocationGroup other) {
		Preconditions.checkNotNull(other);
		
		for (AbstractJobVertex v : this.vertices) {
			v.updateCoLocationGroup(other);
		}
		
		// move vertex membership
		other.vertices.addAll(this.vertices);
		this.vertices.clear();
	}
	
	// --------------------------------------------------------------------------------------------
	
	public CoLocationConstraint getLocationConstraint(int subtask) {
		ensureConstraints(subtask + 1);
		return constraints.get(subtask);
	}
	
	private void ensureConstraints(int num) {
		if (constraints == null) {
			constraints = new ArrayList<CoLocationConstraint>(num);
		} else {
			constraints.ensureCapacity(num);
		}
		
		if (num > constraints.size()) {
			constraints.ensureCapacity(num);
			for (int i = constraints.size(); i < num; i++) {
				constraints.add(new CoLocationConstraint(this));
			}
		}
	}
	
	public AbstractID getId() {
		return id;
	}
	
	public void resetConstraints() {
		for (CoLocationConstraint c : this.constraints) {
			if (!c.isUnassignedOrDisposed()) {
				throw new IllegalStateException("Cannot reset co-location group: some constraints still have executing vertices.");
			}
		}
		this.constraints.clear();
	}
}
