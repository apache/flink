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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

/**
 * Represents an ExecutionVertex.
 */
public interface FailoverVertex {

	/**
	 * Returns the ID of this vertex.
	 *
	 * @return ID of this vertex
	 */
	ExecutionVertexID getExecutionVertexID();

	/**
	 * Returns the name of this vertex.
	 *
	 * @return name of this vertex
	 */
	String getExecutionVertexName();

	/**
	 * Returns all input edges of this vertex.
	 *
	 * @return input edges of this vertex
	 */
	Iterable<? extends FailoverEdge> getInputEdges();

	/**
	 * Returns all output edges of this vertex.
	 *
	 * @return output edges of this vertex
	 */
	Iterable<? extends FailoverEdge> getOutputEdges();
}
