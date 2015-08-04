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

package org.apache.flink.runtime.jobgraph;

/**
 * This class represent edges (communication channels) in a job graph.
 * The edges always go from an intermediate result partition to a job vertex.
 * An edge is parameterized with its {@link DistributionPattern}.
 */
public class JobEdge implements java.io.Serializable {

	private static final long serialVersionUID = 1L;
	
	
	/** The vertex connected to this edge. */
	private final JobVertex target;

	/** The distribution pattern that should be used for this job edge. */
	private final DistributionPattern distributionPattern;
	
	/** The data set at the source of the edge, may be null if the edge is not yet connected*/
	private IntermediateDataSet source;
	
	/** The id of the source intermediate data set */
	private IntermediateDataSetID sourceId;

	/**
	 * Constructs a new job edge, that connects an intermediate result to a consumer task.
	 * 
	 * @param source The data set that is at the source of this edge.
	 * @param target The operation that is at the target of this edge.
	 * @param distributionPattern The pattern that defines how the connection behaves in parallel.
	 */
	public JobEdge(IntermediateDataSet source, JobVertex target, DistributionPattern distributionPattern) {
		if (source == null || target == null || distributionPattern == null) {
			throw new NullPointerException();
		}
		this.target = target;
		this.distributionPattern = distributionPattern;
		this.source = source;
		this.sourceId = source.getId();
	}
	
	/**
	 * Constructs a new job edge that refers to an intermediate result via the Id, rather than directly through
	 * the intermediate data set structure.
	 * 
	 * @param sourceId The id of the data set that is at the source of this edge.
	 * @param target The operation that is at the target of this edge.
	 * @param distributionPattern The pattern that defines how the connection behaves in parallel.
	 */
	public JobEdge(IntermediateDataSetID sourceId, JobVertex target, DistributionPattern distributionPattern) {
		if (sourceId == null || target == null || distributionPattern == null) {
			throw new NullPointerException();
		}
		this.target = target;
		this.distributionPattern = distributionPattern;
		this.sourceId = sourceId;
	}


	/**
	 * Returns the data set at the source of the edge. May be null, if the edge refers to the source via an ID
	 * and has not been connected.
	 * 
	 * @return The data set at the source of the edge
	 */
	public IntermediateDataSet getSource() {
		return source;
	}

	/**
	 * Returns the vertex connected to this edge.
	 * 
	 * @return The vertex connected to this edge.
	 */
	public JobVertex getTarget() {
		return target;
	}
	
	/**
	 * Returns the distribution pattern used for this edge.
	 * 
	 * @return The distribution pattern used for this edge.
	 */
	public DistributionPattern getDistributionPattern(){
		return this.distributionPattern;
	}
	
	/**
	 * Gets the ID of the consumed data set.
	 * 
	 * @return The ID of the consumed data set.
	 */
	public IntermediateDataSetID getSourceId() {
		return sourceId;
	}
	
	public boolean isIdReference() {
		return this.source == null;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public void connecDataSet(IntermediateDataSet dataSet) {
		if (dataSet == null) {
			throw new NullPointerException();
		}
		if (this.source != null) {
			throw new IllegalStateException("The edge is already connected.");
		}
		if (!dataSet.getId().equals(sourceId)) {
			throw new IllegalArgumentException("The data set to connect does not match the sourceId.");
		}
		
		this.source = dataSet;
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return String.format("%s --> %s []", sourceId, target, distributionPattern.name());
	}
}
