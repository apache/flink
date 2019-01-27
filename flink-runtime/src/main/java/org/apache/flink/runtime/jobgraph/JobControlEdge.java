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

package org.apache.flink.runtime.jobgraph;

/**
 * This class represent control edges (not data transmission) in a job graph.
 * The target vertex on the edge is dependent on the source vertex. A control
 * edge is parametrized with its {@link ControlType}.
 */
public class JobControlEdge implements java.io.Serializable {

	private static final long serialVersionUID = 1L;

	private final JobVertex source;
	private final JobVertex target;

	private final ControlType controlType;

	public JobControlEdge(JobVertex source, JobVertex target, ControlType controlType) {
		this.source = source;
		this.target = target;
		this.controlType = controlType;
	}

	public ControlType getControlType() {
		return controlType;
	}

	public JobVertex getSource() {
		return source;
	}

	public JobVertex getTarget() {
		return target;
	}

	@Override
	public String toString() {
		return String.format("%s --> %s [%s]", source, target, controlType.name());
	}
}
