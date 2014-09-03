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


package org.apache.flink.runtime.jobgraph;

/**
 * An abstract base class for output vertices in Nephele.
 * 
 */
public abstract class AbstractJobOutputVertex extends AbstractJobVertex {

	/**
	 * Constructs a new job output vertex with the given name.
	 * 
	 * @param name
	 *        the name of the new job output vertex
	 * @param jobGraph
	 *        the job graph this vertex belongs to
	 */
	protected AbstractJobOutputVertex(String name, JobGraph jobGraph) {
		this(name, null, jobGraph);
	}
	
	protected AbstractJobOutputVertex(String name, JobVertexID id, JobGraph jobGraph) {
		super(name, id, jobGraph);
		jobGraph.addVertex(this);
	}
}
