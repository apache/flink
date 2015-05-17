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

package org.apache.flink.graph.spargel;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.IterationConfiguration;

import java.util.ArrayList;
import java.util.List;

/**
 * A VertexCentricConfiguration object can be used to set the iteration name and
 * degree of parallelism, to register aggregators and use broadcast sets in
 * the {@link org.apache.flink.graph.spargel.VertexUpdateFunction} and {@link org.apache.flink.graph.spargel.MessagingFunction}
 *
 * The VertexCentricConfiguration object is passed as an argument to
 * {@link org.apache.flink.graph.Graph#runVertexCentricIteration (
 * org.apache.flink.graph.spargel.VertexUpdateFunction, org.apache.flink.graph.spargel.MessagingFunction, int,
 * VertexCentricConfiguration)}.
 */
public class VertexCentricConfiguration extends IterationConfiguration {

	/** the broadcast variables for the update function **/
	private List<Tuple2<String, DataSet<?>>> bcVarsUpdate = new ArrayList<Tuple2<String,DataSet<?>>>();

	/** the broadcast variables for the messaging function **/
	private List<Tuple2<String, DataSet<?>>> bcVarsMessaging = new ArrayList<Tuple2<String,DataSet<?>>>();

	/** flag that defines whether the degrees option is set **/
	private boolean optDegrees = false;

	/** flag that defines whether the number of vertices option is set **/
	private boolean optNumVertices = false;

	/** the direction in which the messages should be sent **/
	private EdgeDirection direction = EdgeDirection.OUT;

	public VertexCentricConfiguration() {}

	/**
	 * Adds a data set as a broadcast set to the messaging function.
	 *
	 * @param name The name under which the broadcast data is available in the messaging function.
	 * @param data The data set to be broadcasted.
	 */
	public void addBroadcastSetForMessagingFunction(String name, DataSet<?> data) {
		this.bcVarsMessaging.add(new Tuple2<String, DataSet<?>>(name, data));
	}

	/**
	 * Adds a data set as a broadcast set to the vertex update function.
	 *
	 * @param name The name under which the broadcast data is available in the vertex update function.
	 * @param data The data set to be broadcasted.
	 */
	public void addBroadcastSetForUpdateFunction(String name, DataSet<?> data) {
		this.bcVarsUpdate.add(new Tuple2<String, DataSet<?>>(name, data));
	}

	/**
	 * Get the broadcast variables of the VertexUpdateFunction.
	 *
	 * @return a List of Tuple2, where the first field is the broadcast variable name
	 * and the second field is the broadcast DataSet.
	 */
	public List<Tuple2<String, DataSet<?>>> getUpdateBcastVars() {
		return this.bcVarsUpdate;
	}

	/**
	 * Get the broadcast variables of the MessagingFunction.
	 *
	 * @return a List of Tuple2, where the first field is the broadcast variable name
	 * and the second field is the broadcast DataSet.
	 */
	public List<Tuple2<String, DataSet<?>>> getMessagingBcastVars() {
		return this.bcVarsMessaging;
	}

	// ----------------------------------------------------------------------------------
	// The direction, degrees and the total number of vertices should be optional.
	// The user can access them by setting the direction, degrees or the numVertices options.
	// ----------------------------------------------------------------------------------

	public boolean isOptDegrees() {
		return optDegrees;
	}

	public void setOptDegrees(boolean optDegrees) {
		this.optDegrees = optDegrees;
	}

	public boolean isOptNumVertices() {
		return optNumVertices;
	}

	public void setOptNumVertices(boolean optNumVertices) {
		this.optNumVertices = optNumVertices;
	}

	public EdgeDirection getDirection() {
		return direction;
	}

	public void setDirection(EdgeDirection direction) {
		this.direction = direction;
	}

}
