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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * This class represent edges (communication channels) in a job graph.
 * The edges always go from an intermediate result partition to a job vertex.
 * An edge is parametrized with its {@link DistributionPattern}.
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
	
	/** Optional name for the data shipping strategy (forward, partition hash, rebalance, ...),
	 * to be displayed in the JSON plan */
	private String shipStrategyName;

	/** Optional name for the pre-processing operation (sort, combining sort, ...),
	 * to be displayed in the JSON plan */
	private String preProcessingOperationName;

	/** Optional description of the caching inside an operator, to be displayed in the JSON plan */
	private String operatorLevelCachingDescription;

	/** This cache helps to reduce the calculation load of consumer vertices for each partition */
	private transient Map<Integer, Collection<ExecutionVertexID>> consumerExecutionVerticesCache;
	
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

	/**
	 * Gets the name of the ship strategy for the represented input, like "forward", "partition hash",
	 * "rebalance", "broadcast", ...
	 *
	 * @return The name of the ship strategy for the represented input, or null, if none was set.
	 */
	public String getShipStrategyName() {
		return shipStrategyName;
	}

	/**
	 * Sets the name of the ship strategy for the represented input.
	 *
	 * @param shipStrategyName The name of the ship strategy. 
	 */
	public void setShipStrategyName(String shipStrategyName) {
		this.shipStrategyName = shipStrategyName;
	}

	/**
	 * Gets the name of the pro-processing operation for this input.
	 *
	 * @return The name of the pro-processing operation, or null, if none was set.
	 */
	public String getPreProcessingOperationName() {
		return preProcessingOperationName;
	}

	/**
	 * Sets the name of the pre-processing operation for this input.
	 *
	 * @param preProcessingOperationName The name of the pre-processing operation.
	 */
	public void setPreProcessingOperationName(String preProcessingOperationName) {
		this.preProcessingOperationName = preProcessingOperationName;
	}

	/**
	 * Gets the operator-level caching description for this input.
	 *
	 * @return The description of operator-level caching, or null, is none was set.
	 */
	public String getOperatorLevelCachingDescription() {
		return operatorLevelCachingDescription;
	}

	/**
	 * Sets the operator-level caching description for this input.
	 *
	 * @param operatorLevelCachingDescription The description of operator-level caching.
	 */
	public void setOperatorLevelCachingDescription(String operatorLevelCachingDescription) {
		this.operatorLevelCachingDescription = operatorLevelCachingDescription;
	}

	/**
	 * Clear the cache of consumer execution vertices, should be invoked if a parallelism
	 * change happens to any of the producer or consumer vertices.
	 */
	public void clearConsumerExecutionVerticesCache() {
		if (consumerExecutionVerticesCache != null) {
			consumerExecutionVerticesCache.clear();
		}
	}

	public Collection<ExecutionVertexID> getConsumerExecutionVertices(int partitionNumber) {
		if (consumerExecutionVerticesCache == null) {
			consumerExecutionVerticesCache = new HashMap<>();
		}
		if (consumerExecutionVerticesCache.containsKey(partitionNumber)) {
			return consumerExecutionVerticesCache.get(partitionNumber);
		}

		Collection<ExecutionVertexID> consumers;
		switch (distributionPattern) {
			case POINTWISE:
				consumers = getConsumerExecutionVerticesPointwise(partitionNumber);
				break;
			case ALL_TO_ALL:
				consumers = getConsumerExecutionVerticesAllToAll();
				break;
			default:
				throw new RuntimeException("Unrecognized distribution pattern.");
		}
		consumerExecutionVerticesCache.put(partitionNumber, consumers);
		return consumers;
	}

	private Collection<ExecutionVertexID> getConsumerExecutionVerticesPointwise(int partitionNumber) {
		final int sourceCount = source.getProducer().getParallelism();
		final int targetCount = target.getParallelism();

		final Collection<ExecutionVertexID> consumerVertices = new ArrayList<>();

		// simple case same number of sources as targets
		if (sourceCount == targetCount) {
			consumerVertices.add(new ExecutionVertexID(target.getID(), partitionNumber));
		} else if (sourceCount > targetCount) {
			int vertexSubtaskIndex;

			// check if the pattern is regular or irregular
			// we use int arithmetics for regular, and floating point with rounding for irregular
			if (sourceCount % targetCount == 0) {
				// same number of targets per source
				int factor = sourceCount / targetCount;
				vertexSubtaskIndex = partitionNumber / factor;
			}
			else {
				// different number of targets per source
				float factor = ((float) sourceCount) / targetCount;

				// Do mirror to generate the same edge mapping as in old Flink version
				int mirrorPartitionNumber = sourceCount - 1 - partitionNumber;
				int mirrorVertexSubTaskIndex =  (int) (mirrorPartitionNumber / factor);
				vertexSubtaskIndex = targetCount - 1 - mirrorVertexSubTaskIndex;
			}

			consumerVertices.add(new ExecutionVertexID(target.getID(), vertexSubtaskIndex));
		} else {
			if (targetCount % sourceCount == 0) {
				// same number of targets per source
				int factor = targetCount / sourceCount;
				int startIndex = partitionNumber * factor;

				for (int i = 0; i < factor; i++) {
					consumerVertices.add(new ExecutionVertexID(target.getID(), startIndex + i));
				}
			}
			else {
				float factor = ((float) targetCount) / sourceCount;

				// Do mirror to generate the same edge mapping as in old Flink version
				int mirrorPartitionNumber = sourceCount - 1 - partitionNumber;
				int start = (int) (mirrorPartitionNumber * factor);
				int end = (mirrorPartitionNumber == sourceCount - 1) ?
					targetCount :
					(int) ((mirrorPartitionNumber + 1) * factor);

				for (int i = 0; i < end - start; i++) {
					int mirrorVertexSubTaskIndex = start + i;
					int vertexSubtaskIndex = targetCount - 1 - mirrorVertexSubTaskIndex;
					consumerVertices.add(new ExecutionVertexID(target.getID(), vertexSubtaskIndex));
				}
			}
		}

		return consumerVertices;
	}

	private Collection<ExecutionVertexID> getConsumerExecutionVerticesAllToAll() {
		Collection<ExecutionVertexID> consumerVertices = new ArrayList<>();
		for (int i = 0; i < target.getParallelism(); i++) {
			consumerVertices.add(new ExecutionVertexID(target.getID(), i));
		}
		return consumerVertices;
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return String.format("%s --> %s [%s]", sourceId, target, distributionPattern.name());
	}
}
