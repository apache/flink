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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitSource;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.StoppableTask;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * The base class for job vertexes.
 */
public class JobVertex implements java.io.Serializable {

	private static final long serialVersionUID = 1L;

	private static final String DEFAULT_NAME = "(unnamed vertex)";


	// --------------------------------------------------------------------------------------------
	// Members that define the structure / topology of the graph
	// --------------------------------------------------------------------------------------------

	/** The ID of the vertex. */
	private final JobVertexID id;

	/** List of produced data sets, one per writer */
	private final ArrayList<IntermediateDataSet> results = new ArrayList<IntermediateDataSet>();

	/** List of edges with incoming data. One per Reader. */
	private final ArrayList<JobEdge> inputs = new ArrayList<JobEdge>();

	/** Number of subtasks to split this task into at runtime.*/
	private int parallelism = ExecutionConfig.PARALLELISM_DEFAULT;

	/** Maximum number of subtasks to split this taks into a runtime. */
	private int maxParallelism = Short.MAX_VALUE;

	/** Custom configuration passed to the assigned task at runtime. */
	private Configuration configuration;

	/** The class of the invokable. */
	private String invokableClassName;

	/** Indicates of this job vertex is stoppable or not. */
	private boolean isStoppable = false;

	/** Optionally, a source of input splits */
	private InputSplitSource<?> inputSplitSource;

	/** The name of the vertex. This will be shown in runtime logs and will be in the runtime environment */
	private String name;

	/** Optionally, a sharing group that allows subtasks from different job vertices to run concurrently in one slot */
	private SlotSharingGroup slotSharingGroup;

	/** The group inside which the vertex subtasks share slots */
	private CoLocationGroup coLocationGroup;

	/** Optional, the name of the operator, such as 'Flat Map' or 'Join', to be included in the JSON plan */
	private String operatorName;

	/** Optional, the description of the operator, like 'Hash Join', or 'Sorted Group Reduce',
	 * to be included in the JSON plan */
	private String operatorDescription;

	/** Optional, pretty name of the operator, to be displayed in the JSON plan */
	private String operatorPrettyName;

	/** Optional, the JSON for the optimizer properties of the operator result,
	 * to be included in the JSON plan */
	private String resultOptimizerProperties;

	// --------------------------------------------------------------------------------------------

	/**
	 * Constructs a new job vertex and assigns it with the given name.
	 * 
	 * @param name The name of the new job vertex.
	 */
	public JobVertex(String name) {
		this(name, null);
	}

	/**
	 * Constructs a new job vertex and assigns it with the given name.
	 * 
	 * @param name The name of the new job vertex.
	 * @param id The id of the job vertex.
	 */
	public JobVertex(String name, JobVertexID id) {
		this.name = name == null ? DEFAULT_NAME : name;
		this.id = id == null ? new JobVertexID() : id;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the ID of this job vertex.
	 * 
	 * @return The ID of this job vertex
	 */
	public JobVertexID getID() {
		return this.id;
	}

	/**
	 * Returns the name of the vertex.
	 * 
	 * @return The name of the vertex.
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * Sets the name of the vertex
	 * 
	 * @param name The new name.
	 */
	public void setName(String name) {
		this.name = name == null ? DEFAULT_NAME : name;
	}

	/**
	 * Returns the number of produced intermediate data sets.
	 * 
	 * @return The number of produced intermediate data sets.
	 */
	public int getNumberOfProducedIntermediateDataSets() {
		return this.results.size();
	}

	/**
	 * Returns the number of inputs.
	 * 
	 * @return The number of inputs.
	 */
	public int getNumberOfInputs() {
		return this.inputs.size();
	}

	/**
	 * Returns the vertex's configuration object which can be used to pass custom settings to the task at runtime.
	 * 
	 * @return the vertex's configuration object
	 */
	public Configuration getConfiguration() {
		if (this.configuration == null) {
			this.configuration = new Configuration();
		}
		return this.configuration;
	}

	public void setInvokableClass(Class<? extends AbstractInvokable> invokable) {
		Preconditions.checkNotNull(invokable);
		this.invokableClassName = invokable.getName();
		this.isStoppable = StoppableTask.class.isAssignableFrom(invokable);
	}

	/**
	 * Returns the name of the invokable class which represents the task of this vertex.
	 * 
	 * @return The name of the invokable class, <code>null</code> if not set.
	 */
	public String getInvokableClassName() {
		return this.invokableClassName;
	}

	/**
	 * Returns the invokable class which represents the task of this vertex
	 * 
	 * @param cl The classloader used to resolve user-defined classes
	 * @return The invokable class, <code>null</code> if it is not set
	 */
	public Class<? extends AbstractInvokable> getInvokableClass(ClassLoader cl) {
		if (cl == null) {
			throw new NullPointerException("The classloader must not be null.");
		}
		if (invokableClassName == null) {
			return null;
		}

		try {
			return Class.forName(invokableClassName, true, cl).asSubclass(AbstractInvokable.class);
		}
		catch (ClassNotFoundException e) {
			throw new RuntimeException("The user-code class could not be resolved.", e);
		}
		catch (ClassCastException e) {
			throw new RuntimeException("The user-code class is no subclass of " + AbstractInvokable.class.getName(), e);
		}
	}

	/**
	 * Gets the parallelism of the task.
	 * 
	 * @return The parallelism of the task.
	 */
	public int getParallelism() {
		return parallelism;
	}

	/**
	 * Sets the parallelism for the task.
	 * 
	 * @param parallelism The parallelism for the task.
	 */
	public void setParallelism(int parallelism) {
		if (parallelism < 1) {
			throw new IllegalArgumentException("The parallelism must be at least one.");
		}
		this.parallelism = parallelism;
	}

	/**
	 * Gets the maximum parallelism for the task.
	 *
	 * @return The maximum parallelism for the task.
	 */
	public int getMaxParallelism() {
		return maxParallelism;
	}

	/**
	 * Sets the maximum parallelism for the task.
	 *
	 * @param maxParallelism The maximum parallelism to be set.
	 */
	public void setMaxParallelism(int maxParallelism) {
		org.apache.flink.util.Preconditions.checkArgument(
				maxParallelism > 0 && maxParallelism <= Short.MAX_VALUE, "The max parallelism must be at least 1.");

		this.maxParallelism = maxParallelism;
	}

	public InputSplitSource<?> getInputSplitSource() {
		return inputSplitSource;
	}

	public void setInputSplitSource(InputSplitSource<?> inputSplitSource) {
		this.inputSplitSource = inputSplitSource;
	}

	public List<IntermediateDataSet> getProducedDataSets() {
		return this.results;
	}

	public List<JobEdge> getInputs() {
		return this.inputs;
	}

	/**
	 * Associates this vertex with a slot sharing group for scheduling. Different vertices in the same
	 * slot sharing group can run one subtask each in the same slot.
	 * 
	 * @param grp The slot sharing group to associate the vertex with.
	 */
	public void setSlotSharingGroup(SlotSharingGroup grp) {
		if (this.slotSharingGroup != null) {
			this.slotSharingGroup.removeVertexFromGroup(id);
		}

		this.slotSharingGroup = grp;
		if (grp != null) {
			grp.addVertexToGroup(id);
		}
	}

	/**
	 * Gets the slot sharing group that this vertex is associated with. Different vertices in the same
	 * slot sharing group can run one subtask each in the same slot. If the vertex is not associated with
	 * a slot sharing group, this method returns {@code null}.
	 * 
	 * @return The slot sharing group to associate the vertex with, or {@code null}, if not associated with one.
	 */
	public SlotSharingGroup getSlotSharingGroup() {
		return slotSharingGroup;
	}

	/**
	 * Tells this vertex to strictly co locate its subtasks with the subtasks of the given vertex.
	 * Strict co-location implies that the n'th subtask of this vertex will run on the same parallel computing
	 * instance (TaskManager) as the n'th subtask of the given vertex.
	 * 
	 * NOTE: Co-location is only possible between vertices in a slot sharing group.
	 * 
	 * NOTE: This vertex must (transitively) depend on the vertex to be co-located with. That means that the
	 * respective vertex must be a (transitive) input of this vertex.
	 * 
	 * @param strictlyCoLocatedWith The vertex whose subtasks to co-locate this vertex's subtasks with.
	 * 
	 * @throws IllegalArgumentException Thrown, if this vertex and the vertex to co-locate with are not in a common
	 *                                  slot sharing group.
	 * 
	 * @see #setSlotSharingGroup(SlotSharingGroup)
	 */
	public void setStrictlyCoLocatedWith(JobVertex strictlyCoLocatedWith) {
		if (this.slotSharingGroup == null || this.slotSharingGroup != strictlyCoLocatedWith.slotSharingGroup) {
			throw new IllegalArgumentException("Strict co-location requires that both vertices are in the same slot sharing group.");
		}

		CoLocationGroup thisGroup = this.coLocationGroup;
		CoLocationGroup otherGroup = strictlyCoLocatedWith.coLocationGroup;

		if (otherGroup == null) {
			if (thisGroup == null) {
				CoLocationGroup group = new CoLocationGroup(this, strictlyCoLocatedWith);
				this.coLocationGroup = group;
				strictlyCoLocatedWith.coLocationGroup = group;
			}
			else {
				thisGroup.addVertex(strictlyCoLocatedWith);
				strictlyCoLocatedWith.coLocationGroup = thisGroup;
			}
		}
		else {
			if (thisGroup == null) {
				otherGroup.addVertex(this);
				this.coLocationGroup = otherGroup;
			}
			else {
				// both had yet distinct groups, we need to merge them
				thisGroup.mergeInto(otherGroup);
			}
		}
	}

	public CoLocationGroup getCoLocationGroup() {
		return coLocationGroup;
	}

	public void updateCoLocationGroup(CoLocationGroup group) {
		this.coLocationGroup = group;
	}

	// --------------------------------------------------------------------------------------------

	public IntermediateDataSet createAndAddResultDataSet(ResultPartitionType partitionType) {
		return createAndAddResultDataSet(new IntermediateDataSetID(), partitionType);
	}

	public IntermediateDataSet createAndAddResultDataSet(
			IntermediateDataSetID id,
			ResultPartitionType partitionType) {

		IntermediateDataSet result = new IntermediateDataSet(id, partitionType, this);
		this.results.add(result);
		return result;
	}

	public JobEdge connectDataSetAsInput(IntermediateDataSet dataSet, DistributionPattern distPattern) {
		JobEdge edge = new JobEdge(dataSet, this, distPattern);
		this.inputs.add(edge);
		dataSet.addConsumer(edge);
		return edge;
	}

	public JobEdge connectNewDataSetAsInput(JobVertex input, DistributionPattern distPattern) {
		return connectNewDataSetAsInput(input, distPattern, ResultPartitionType.PIPELINED, false);
	}

	public JobEdge connectNewDataSetAsInput(
			JobVertex input,
			DistributionPattern distPattern,
			ResultPartitionType partitionType) {

		return connectNewDataSetAsInput(input, distPattern, partitionType, false);
	}

	public JobEdge connectNewDataSetAsInput(
			JobVertex input,
			DistributionPattern distPattern,
			ResultPartitionType partitionType,
			boolean eagerlyDeployConsumers) {

		IntermediateDataSet dataSet = input.createAndAddResultDataSet(partitionType);
		dataSet.setEagerlyDeployConsumers(eagerlyDeployConsumers);

		JobEdge edge = new JobEdge(dataSet, this, distPattern);
		this.inputs.add(edge);
		dataSet.addConsumer(edge);
		return edge;
	}

	public void connectIdInput(IntermediateDataSetID dataSetId, DistributionPattern distPattern) {
		JobEdge edge = new JobEdge(dataSetId, this, distPattern);
		this.inputs.add(edge);
	}

	// --------------------------------------------------------------------------------------------

	public boolean isInputVertex() {
		return this.inputs.isEmpty();
	}

	public boolean isStoppable() {
		return this.isStoppable;
	}

	public boolean isOutputVertex() {
		return this.results.isEmpty();
	}

	public boolean hasNoConnectedInputs() {
		for (JobEdge edge : inputs) {
			if (!edge.isIdReference()) {
				return false;
			}
		}

		return true;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * A hook that can be overwritten by sub classes to implement logic that is called by the
	 * master when the job starts.
	 * 
	 * @param loader The class loader for user defined code.
	 * @throws Exception The method may throw exceptions which cause the job to fail immediately.
	 */
	public void initializeOnMaster(ClassLoader loader) throws Exception {}

	/**
	 * A hook that can be overwritten by sub classes to implement logic that is called by the
	 * master after the job completed.
	 * 
	 * @param loader The class loader for user defined code.
	 * @throws Exception The method may throw exceptions which cause the job to fail immediately.
	 */
	public void finalizeOnMaster(ClassLoader loader) throws Exception {}

	// --------------------------------------------------------------------------------------------

	public String getOperatorName() {
		return operatorName;
	}

	public void setOperatorName(String operatorName) {
		this.operatorName = operatorName;
	}

	public String getOperatorDescription() {
		return operatorDescription;
	}

	public void setOperatorDescription(String operatorDescription) {
		this.operatorDescription = operatorDescription;
	}

	public void setOperatorPrettyName(String operatorPrettyName) {
		this.operatorPrettyName = operatorPrettyName;
	}

	public String getOperatorPrettyName() {
		return operatorPrettyName;
	}

	public String getResultOptimizerProperties() {
		return resultOptimizerProperties;
	}

	public void setResultOptimizerProperties(String resultOptimizerProperties) {
		this.resultOptimizerProperties = resultOptimizerProperties;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return this.name + " (" + this.invokableClassName + ')';
	}
}
