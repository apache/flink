/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.runtime.jobgraph.ControlType;
import org.apache.flink.runtime.jobgraph.EdgeID;

import java.util.Objects;

/**
 * An control edge in the streaming topology to expresses scheduling dependencies.
 * Data is not transmitted on the edge and the target vertex on the edge is dependent
 * on the source vertex. One edge like this does not necessarily gets converted to a
 * connection between two job vertices (due to chaining/optimization).
 */
public class StreamControlEdge implements java.io.Serializable {

	private static final long serialVersionUID = 1L;

	private final EdgeID edgeID;

	private final String edgeName;

	private final StreamNode sourceVertex;
	private final StreamNode targetVertex;

	private final ControlType controlType;

	public StreamControlEdge(StreamNode sourceVertex, StreamNode targetVertex, ControlType controlType) {
		this.sourceVertex = sourceVertex;
		this.targetVertex = targetVertex;
		this.controlType = controlType;

		this.edgeID = new EdgeID();
		this.edgeName = sourceVertex + "_" + targetVertex + "_" + controlType;
	}

	public EdgeID getEdgeID() {
		return this.edgeID;
	}

	public StreamNode getSourceVertex() {
		return this.sourceVertex;
	}

	public StreamNode getTargetVertex() {
		return this.targetVertex;
	}

	@Override
	public int hashCode() {
		return Objects.hash(edgeName, edgeID);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		StreamControlEdge that = (StreamControlEdge) o;

		return edgeName.equals(that.edgeName) && edgeID.equals(that.getEdgeID());
	}

	@Override
	public String toString() {
		return "(" + sourceVertex.getId() + " -> " + targetVertex.getId() +
				", controlType=" + controlType.name() + ", edgeID=" + edgeID + ')';
	}
}
