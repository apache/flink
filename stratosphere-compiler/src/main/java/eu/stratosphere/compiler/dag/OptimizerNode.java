/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.compiler.dag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.stratosphere.api.common.operators.AbstractUdfOperator;
import eu.stratosphere.api.common.operators.CompilerHints;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.common.operators.util.FieldSet;
import eu.stratosphere.compiler.CompilerException;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.costs.CostEstimator;
import eu.stratosphere.compiler.dataproperties.InterestingProperties;
import eu.stratosphere.compiler.dataproperties.RequestedGlobalProperties;
import eu.stratosphere.compiler.dataproperties.RequestedLocalProperties;
import eu.stratosphere.compiler.plan.PlanNode;
import eu.stratosphere.compiler.plandump.DumpableConnection;
import eu.stratosphere.compiler.plandump.DumpableNode;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.util.Visitable;
import eu.stratosphere.util.Visitor;

/**
 * This class represents a node in the optimizer's internal representation of the PACT plan. It contains
 * extra information about estimates, hints and data properties.
 */
public abstract class OptimizerNode implements Visitable<OptimizerNode>, EstimateProvider, DumpableNode<OptimizerNode> {
	
	public static final int MAX_DYNAMIC_PATH_COST_WEIGHT = 100;
	
	// --------------------------------------------------------------------------------------------
	//                                      Members
	// --------------------------------------------------------------------------------------------

	private final Operator<?> pactContract; // The operator (Reduce / Join / DataSource / ...)
	
	private List<String> broadcastConnectionNames = new ArrayList<String>(); // the broadcast inputs names of this node
	
	private List<PactConnection> broadcastConnections = new ArrayList<PactConnection>(); // the broadcast inputs of this node
	
	private List<PactConnection> outgoingConnections; // The links to succeeding nodes

	private InterestingProperties intProps; // the interesting properties of this node
	
	// --------------------------------- Branch Handling ------------------------------------------

	protected List<UnclosedBranchDescriptor> openBranches; // stack of branches in the sub-graph that are not joined
	
	protected Set<OptimizerNode> closedBranchingNodes; 	// stack of branching nodes which have already been closed
	
	protected List<OptimizerNode> hereJoinedBranches;	// the branching nodes (node with multiple outputs)
	// 										that are partially joined (through multiple inputs or broadcast vars)

	// ---------------------------- Estimates and Annotations -------------------------------------
	
	protected long estimatedOutputSize = -1; // the estimated size of the output (bytes)

	protected long estimatedNumRecords = -1; // the estimated number of key/value pairs in the output
	
	protected Set<FieldSet> uniqueFields; // set of attributes that will always be unique after this node

	// --------------------------------- General Parameters ---------------------------------------
	
	private int degreeOfParallelism = -1; // the number of parallel instances of this node

	private int subtasksPerInstance = -1; // the number of parallel instance that will run on the same machine
	
	private long minimalMemoryPerSubTask = -1;

	protected int id = -1; 				// the id for this node.
	
	protected int costWeight = 1;		// factor to weight the costs for dynamic paths
	
	protected boolean onDynamicPath;
	
	protected List<PlanNode> cachedPlans;	// cache candidates, because the may be accessed repeatedly
	
	protected int[][] remappedKeys;

	// ------------------------------------------------------------------------
	//                      Constructor / Setup
	// ------------------------------------------------------------------------

	/**
	 * Creates a new node for the optimizer plan.
	 * 
	 * @param op The operator that the node represents.
	 */
	public OptimizerNode(Operator<?> op) {
		this.pactContract = op;
		readStubAnnotations();
		
		if (this.pactContract instanceof AbstractUdfOperator) {
			final AbstractUdfOperator<?, ?> pact = (AbstractUdfOperator<?, ?>) this.pactContract;
			this.remappedKeys = new int[pact.getNumberOfInputs()][];
			for (int i = 0; i < this.remappedKeys.length; i++) {
				final int[] keys = pact.getKeyColumns(i);
				int[] rk = new int[keys.length];
				System.arraycopy(keys, 0, rk, 0, keys.length);
				this.remappedKeys[i] = rk;
			}
		}
	}
	
	protected OptimizerNode(OptimizerNode toCopy) {
		this.pactContract = toCopy.pactContract;
		
		this.intProps = toCopy.intProps;
		
		this.remappedKeys = toCopy.remappedKeys;
		
		this.openBranches = toCopy.openBranches;
		this.closedBranchingNodes = toCopy.closedBranchingNodes;
		
		this.estimatedOutputSize = toCopy.estimatedOutputSize;
		this.estimatedNumRecords = toCopy.estimatedNumRecords;
		
		this.degreeOfParallelism = toCopy.degreeOfParallelism;
		this.subtasksPerInstance = toCopy.subtasksPerInstance;
		this.minimalMemoryPerSubTask = toCopy.minimalMemoryPerSubTask;
		
		this.id = toCopy.id;
		this.costWeight = toCopy.costWeight;
		this.onDynamicPath = toCopy.onDynamicPath;
	}

	// ------------------------------------------------------------------------
	//      Abstract methods that implement node specific behavior
	//        and the pact type specific optimization methods.
	// ------------------------------------------------------------------------

	/**
	 * Gets the name of this node. This returns either the name of the PACT, or
	 * a string marking the node as a data source or a data sink.
	 * 
	 * @return The node name.
	 */
	public abstract String getName();

	/**
	 * This function is for plan translation purposes. Upon invocation, the implementing subclasses should
	 * examine its contained contract and look at the contracts that feed their data into that contract.
	 * The method should then create a <tt>PactConnection</tt> for each of those inputs.
	 * <p>
	 * In addition, the nodes must set the shipping strategy of the connection, if a suitable optimizer hint is found.
	 * 
	 * @param contractToNode
	 *        The map to translate the contracts to their corresponding optimizer nodes.
	 */
	public abstract void setInput(Map<Operator<?>, OptimizerNode> contractToNode);

	/**
	 * This function is for plan translation purposes. Upon invocation, this method creates a {@link PactConnection}
	 * for each one of the broadcast inputs associated with the {@code Operator} referenced by this node.
	 * <p>
	 * The {@code PactConnections} must set its shipping strategy type to BROADCAST.
	 * 
	 * @param operatorToNode
	 *        The map associating operators with their corresponding optimizer nodes.
	 * @throws CompilerException
	 */
	public void setBroadcastInputs(Map<Operator<?>, OptimizerNode> operatorToNode) throws CompilerException {

		// skip for Operators that don't support broadcast variables 
		if (!(getPactContract() instanceof AbstractUdfOperator<?, ?>)) {
			return;
		}

		// get all broadcast inputs
		AbstractUdfOperator<?, ?> operator = ((AbstractUdfOperator<?, ?>) getPactContract());

		// create connections and add them
		for (Map.Entry<String, Operator<?>> input : operator.getBroadcastInputs().entrySet()) {
			OptimizerNode predecessor = operatorToNode.get(input.getValue());
			PactConnection connection = new PactConnection(predecessor, this, ShipStrategyType.BROADCAST);
			addBroadcastConnection(input.getKey(), connection);
			predecessor.addOutgoingConnection(connection);
		}
	}

	/**
	 * This method needs to be overridden by subclasses to return the children.
	 * 
	 * @return The list of incoming links.
	 */
	public abstract List<PactConnection> getIncomingConnections();

	/**
	 * Tells the node to compute the interesting properties for its inputs. The interesting properties
	 * for the node itself must have been computed before.
	 * The node must then see how many of interesting properties it preserves and add its own.
	 * 
	 * @param estimator	The {@code CostEstimator} instance to use for plan cost estimation.
	 */
	public abstract void computeInterestingPropertiesForInputs(CostEstimator estimator);

	/**
	 * This method causes the node to compute the description of open branches in its sub-plan. An open branch
	 * describes, that a (transitive) child node had multiple outputs, which have not all been re-joined in the
	 * sub-plan. This method needs to set the <code>openBranches</code> field to a stack of unclosed branches, the
	 * latest one top. A branch is considered closed, if some later node sees all of the branching node's outputs,
	 * no matter if there have been more branches to different paths in the meantime.
	 */
	public abstract void computeUnclosedBranchStack();
	
	
	protected List<UnclosedBranchDescriptor> computeUnclosedBranchStackForBroadcastInputs(List<UnclosedBranchDescriptor> branchesSoFar) {
		// handle the data flow branching for the broadcast inputs
		for (PactConnection broadcastInput : getBroadcastConnections()) {
			OptimizerNode bcSource = broadcastInput.getSource();
			addClosedBranches(bcSource.closedBranchingNodes);
			
			List<UnclosedBranchDescriptor> bcBranches = bcSource.getBranchesForParent(broadcastInput);
			
			ArrayList<UnclosedBranchDescriptor> mergedBranches = new ArrayList<UnclosedBranchDescriptor>();
			mergeLists(branchesSoFar, bcBranches, mergedBranches);
			branchesSoFar = mergedBranches.isEmpty() ? Collections.<UnclosedBranchDescriptor>emptyList() : mergedBranches;
		}
		
		return branchesSoFar;
	}

	/**
	 * Computes the plan alternatives for this node, an implicitly for all nodes that are children of
	 * this node. This method must determine for each alternative the global and local properties
	 * and the costs. This method may recursively call <code>getAlternatives()</code> on its children
	 * to get their plan alternatives, and build its own alternatives on top of those.
	 * 
	 * @param estimator
	 *        The cost estimator used to estimate the costs of each plan alternative.
	 * @return A list containing all plan alternatives.
	 */
	public abstract List<PlanNode> getAlternativePlans(CostEstimator estimator);

	/**
	 * This method implements the visit of a depth-first graph traversing visitor. Implementors must first
	 * call the <code>preVisit()</code> method, then hand the visitor to their children, and finally call
	 * the <code>postVisit()</code> method.
	 * 
	 * @param visitor
	 *        The graph traversing visitor.
	 * @see eu.stratosphere.util.Visitable#accept(eu.stratosphere.util.Visitor)
	 */
	@Override
	public abstract void accept(Visitor<OptimizerNode> visitor);

	/**
	 * Checks, whether this node requires memory for its tasks or not.
	 * 
	 * @return True, if this node contains logic that requires memory usage, false otherwise.
	 */
	public abstract boolean isMemoryConsumer();
	
	/**
	 * Checks whether a field is modified by the user code or whether it is kept unchanged.
	 * 
	 * @param input The input number.
	 * @param fieldNumber The position of the field.
	 * 
	 * @return True if the field is not changed by the user function, false otherwise.
	 */
	public abstract boolean isFieldConstant(int input, int fieldNumber);

	// ------------------------------------------------------------------------
	//                          Getters / Setters
	// ------------------------------------------------------------------------

	@Override
	public Iterable<OptimizerNode> getPredecessors() {
		List<OptimizerNode> allPredecessors = new ArrayList<OptimizerNode>();
		
		for (Iterator<PactConnection> inputs = getIncomingConnections().iterator(); inputs.hasNext(); ){
			allPredecessors.add(inputs.next().getSource());
		}
		
		for (PactConnection conn : getBroadcastConnections()) {
			allPredecessors.add(conn.getSource());
		}
		
		return allPredecessors;
	}
	
	/**
	 * Gets the ID of this node. If the id has not yet been set, this method returns -1;
	 * 
	 * @return This node's id, or -1, if not yet set.
	 */
	public int getId() {
		return this.id;
	}

	/**
	 * Sets the ID of this node.
	 * 
	 * @param id
	 *        The id for this node.
	 */
	public void initId(int id) {
		if (id <= 0) {
			throw new IllegalArgumentException();
		}
		
		if (this.id == -1) {
			this.id = id;
		} else {
			throw new IllegalStateException("Id has already been initialized.");
		}
	}

	/**
	 * Adds the broadcast connection identified by the given {@code name} to this node.
	 * 
	 * @param broadcastConnection
	 *        The connection to add.
	 */
	public void addBroadcastConnection(String name, PactConnection broadcastConnection) {
		this.broadcastConnectionNames.add(name);
		this.broadcastConnections.add(broadcastConnection);
	}

	/**
	 * Return the list of names associated with broadcast inputs for this node.
	 */
	public List<String> getBroadcastConnectionNames() {
		return this.broadcastConnectionNames;
	}

	/**
	 * Return the list of inputs associated with broadcast variables for this node.
	 */
	public List<PactConnection> getBroadcastConnections() {
		return this.broadcastConnections;
	}

	/**
	 * Adds a new outgoing connection to this node.
	 * 
	 * @param pactConnection
	 *        The connection to add.
	 */
	public void addOutgoingConnection(PactConnection pactConnection) {
		if (this.outgoingConnections == null) {
			this.outgoingConnections = new ArrayList<PactConnection>();
		} else {
			if (this.outgoingConnections.size() == 64) {
				throw new CompilerException("Cannot currently handle nodes with more than 64 outputs.");
			}
		}

		this.outgoingConnections.add(pactConnection);
	}

	/**
	 * The list of outgoing connections from this node to succeeding tasks.
	 * 
	 * @return The list of outgoing connections.
	 */
	public List<PactConnection> getOutgoingConnections() {
		return this.outgoingConnections;
	}

	/**
	 * Gets the object that specifically describes the contract of this node.
	 * 
	 * @return This node's contract.
	 */
	public Operator<?> getPactContract() {
		return this.pactContract;
	}

	/**
	 * Gets the degree of parallelism for the contract represented by this optimizer node.
	 * The degree of parallelism denotes how many parallel instances of the user function will be
	 * spawned during the execution. If this value is <code>-1</code>, then the system will take
	 * the default number of parallel instances.
	 * 
	 * @return The degree of parallelism.
	 */
	public int getDegreeOfParallelism() {
		return this.degreeOfParallelism;
	}

	/**
	 * Sets the degree of parallelism for the contract represented by this optimizer node.
	 * The degree of parallelism denotes how many parallel instances of the user function will be
	 * spawned during the execution. If this value is set to <code>-1</code>, then the system will take
	 * the default number of parallel instances.
	 * 
	 * @param degreeOfParallelism
	 *        The degree of parallelism to set.
	 * @throws IllegalArgumentException
	 *         If the degree of parallelism is smaller than one.
	 */
	public void setDegreeOfParallelism(int degreeOfParallelism) {
		if (degreeOfParallelism < 1) {
			throw new IllegalArgumentException();
		}
		this.degreeOfParallelism = degreeOfParallelism;
	}

	/**
	 * Gets the number of parallel instances of the contract that are
	 * to be executed on the same compute instance (logical machine).
	 * 
	 * @return The number of subtask instances per machine.
	 */
	public int getSubtasksPerInstance() {
		return this.subtasksPerInstance;
	}

	/**
	 * Sets the number of parallel task instances of the contract that are
	 * to be executed on the same computing instance (logical machine).
	 * 
	 * @param instancesPerMachine The instances per machine.
	 * @throws IllegalArgumentException If the number of instances per machine is smaller than one.
	 */
	public void setSubtasksPerInstance(int instancesPerMachine) {
		if (instancesPerMachine < 1) {
			throw new IllegalArgumentException();
		}
		this.subtasksPerInstance = instancesPerMachine;
	}
	
	/**
	 * Gets the minimal guaranteed memory per subtask for tasks represented by this OptimizerNode.
	 *
	 * @return The minimal guaranteed memory per subtask, in bytes.
	 */
	public long getMinimalMemoryPerSubTask() {
		return this.minimalMemoryPerSubTask;
	}
	
	/**
	 * Sets the minimal guaranteed memory per subtask for tasks represented by this OptimizerNode.
	 *
	 * @param minimalGuaranteedMemory The minimal guaranteed memory per subtask, in bytes.
	 */
	public void setMinimalMemoryPerSubTask(long minimalGuaranteedMemory) {
		this.minimalMemoryPerSubTask = minimalGuaranteedMemory;
	}
	
	/**
	 * Gets the amount of memory that all subtasks of this task have jointly available.
	 * 
	 * @return The total amount of memory across all subtasks.
	 */
	public long getMinimalMemoryAcrossAllSubTasks() {
		return this.minimalMemoryPerSubTask == -1 ? -1 : this.minimalMemoryPerSubTask * this.degreeOfParallelism;
	}
	
	public boolean isOnDynamicPath() {
		return this.onDynamicPath;
	}
	
	public void identifyDynamicPath(int costWeight) {
		boolean anyDynamic = false;
		boolean allDynamic = true;
		
		for (PactConnection conn : getIncomingConnections()) {
			boolean dynamicIn = conn.isOnDynamicPath();
			anyDynamic |= dynamicIn;
			allDynamic &= dynamicIn;
		}
		
		for (PactConnection conn : getBroadcastConnections()) {
			boolean dynamicIn = conn.isOnDynamicPath();
			anyDynamic |= dynamicIn;
			allDynamic &= dynamicIn;
		}
		
		if (anyDynamic) {
			this.onDynamicPath = true;
			this.costWeight = costWeight;
			if (!allDynamic) {
				// this node joins static and dynamic path.
				// mark the connections where the source is not dynamic as cached
				for (PactConnection conn : getIncomingConnections()) {
					if (!conn.getSource().isOnDynamicPath()) {
						conn.setMaterializationMode(conn.getMaterializationMode().makeCached());
					}
				}
				
				// broadcast variables are always cached, because they stay unchanged available in the
				// runtime context of the functions
			}
		}
	}
	
	public int getCostWeight() {
		return this.costWeight;
	}
	
	public int getMaxDepth() {
		int maxDepth = 0;
		for (PactConnection conn : getIncomingConnections()) {
			maxDepth = Math.max(maxDepth, conn.getMaxDepth());
		}
		for (PactConnection conn : getBroadcastConnections()) {
			maxDepth = Math.max(maxDepth, conn.getMaxDepth());
		}
		
		return maxDepth;
	}

	/**
	 * Gets the properties that are interesting for this node to produce.
	 * 
	 * @return The interesting properties for this node, or null, if not yet computed.
	 */
	public InterestingProperties getInterestingProperties() {
		return this.intProps;
	}
	
	
	public long getEstimatedOutputSize() {
		return this.estimatedOutputSize;
	}

	public long getEstimatedNumRecords() {
		return this.estimatedNumRecords;
	}
	
	public float getEstimatedAvgWidthPerOutputRecord() {
		if (this.estimatedOutputSize > 0 && this.estimatedNumRecords > 0) {
			return ((float) this.estimatedOutputSize) / this.estimatedNumRecords;
		} else {
			return -1.0f;
		}
	}

	/**
	 * Checks whether this node has branching output. A node's output is branched, if it has more
	 * than one output connection.
	 * 
	 * @return True, if the node's output branches. False otherwise.
	 */
	public boolean isBranching() {
		return getOutgoingConnections() != null && getOutgoingConnections().size() > 1;
	}

	// ------------------------------------------------------------------------
	//                              Miscellaneous
	// ------------------------------------------------------------------------

	/**
	 * Checks, if all outgoing connections have their interesting properties set from their target nodes.
	 * 
	 * @return True, if on all outgoing connections, the interesting properties are set. False otherwise.
	 */
	public boolean haveAllOutputConnectionInterestingProperties() {
		for (PactConnection conn : getOutgoingConnections()) {
			if (conn.getInterestingProperties() == null) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Computes all the interesting properties that are relevant to this node. The interesting
	 * properties are a union of the interesting properties on each outgoing connection.
	 * However, if two interesting properties on the outgoing connections overlap,
	 * the interesting properties will occur only once in this set. For that, this
	 * method deduplicates and merges the interesting properties.
	 * This method returns copies of the original interesting properties objects and
	 * leaves the original objects, contained by the connections, unchanged.
	 */
	public void computeUnionOfInterestingPropertiesFromSuccessors() {
		List<PactConnection> conns = getOutgoingConnections();
		if (conns.size() == 0) {
			// no incoming, we have none ourselves
			this.intProps = new InterestingProperties();
		} else {
			this.intProps = conns.get(0).getInterestingProperties().clone();
			for (int i = 1; i < conns.size(); i++) {
				this.intProps.addInterestingProperties(conns.get(i).getInterestingProperties());
			}
		}
		this.intProps.dropTrivials();
	}
	
	public void clearInterestingProperties() {
		this.intProps = null;
		for (PactConnection conn : getIncomingConnections()) {
			conn.clearInterestingProperties();
		}
		for (PactConnection conn : getBroadcastConnections()) {
			conn.clearInterestingProperties();
		}
	}
	
	/**
	 * Causes this node to compute its output estimates (such as number of rows, size in bytes)
	 * based on the inputs and the compiler hints. The compiler hints are instantiated with conservative
	 * default values which are used if no other values are provided. Nodes may access the statistics to
	 * determine relevant information.
	 * 
	 * @param statistics
	 *        The statistics object which may be accessed to get statistical information.
	 *        The parameter may be null, if no statistics are available.
	 */
	public void computeOutputEstimates(DataStatistics statistics) {
		// sanity checking
		for (PactConnection c : getIncomingConnections()) {
			if (c.getSource() == null) {
				throw new CompilerException("Bug: Estimate computation called before inputs have been set.");
			}
		}
		
		// let every operator do its computation
		computeOperatorSpecificDefaultEstimates(statistics);
		
		if (this.estimatedOutputSize < 0) {
			this.estimatedOutputSize = -1;
		}
		if (this.estimatedNumRecords < 0) {
			this.estimatedNumRecords = -1;
		}
		
		// overwrite default estimates with hints, if given
		if (getPactContract() == null || getPactContract().getCompilerHints() == null) {
			return ;
		}
		
		CompilerHints hints = getPactContract().getCompilerHints();
		if (hints.getOutputSize() >= 0) {
			this.estimatedOutputSize = hints.getOutputSize();
		}
		
		if (hints.getOutputCardinality() >= 0) {
			this.estimatedNumRecords = hints.getOutputCardinality();
		}
		
		if (hints.getFilterFactor() >= 0.0f) {
			if (this.estimatedNumRecords >= 0) {
				this.estimatedNumRecords = (long) (this.estimatedNumRecords * hints.getFilterFactor());
				
				if (this.estimatedOutputSize >= 0) {
					this.estimatedOutputSize = (long) (this.estimatedOutputSize * hints.getFilterFactor());
				}
			}
			else if (this instanceof SingleInputNode) {
				OptimizerNode pred = ((SingleInputNode) this).getPredecessorNode();
				if (pred != null && pred.getEstimatedNumRecords() >= 0) {
					this.estimatedNumRecords = (long) (pred.getEstimatedNumRecords() * hints.getFilterFactor());
				}
			}
		}
		
		// use the width to infer the cardinality (given size) and vice versa
		if (hints.getAvgOutputRecordSize() >= 1) {
			// the estimated number of rows based on size
			if (this.estimatedNumRecords == -1 && this.estimatedOutputSize >= 0) {
				this.estimatedNumRecords = (long) (this.estimatedOutputSize / hints.getAvgOutputRecordSize());
			}
			else if (this.estimatedOutputSize == -1 && this.estimatedNumRecords >= 0) {
				this.estimatedOutputSize = (long) (this.estimatedNumRecords * hints.getAvgOutputRecordSize());
			}
		}
	}
	
	protected abstract void computeOperatorSpecificDefaultEstimates(DataStatistics statistics);
	
	// ------------------------------------------------------------------------
	// Reading of stub annotations
	// ------------------------------------------------------------------------
	
	/**
	 * Reads all stub annotations, i.e. which fields remain constant, what cardinality bounds the
	 * functions have, which fields remain unique.
	 */
	protected void readStubAnnotations() {
		readUniqueFieldsAnnotation();
	}
	
	protected void readUniqueFieldsAnnotation() {
		if (this.pactContract.getCompilerHints() != null) {
			Set<FieldSet> uniqueFieldSets = pactContract.getCompilerHints().getUniqueFields();
			if (uniqueFieldSets != null) {
				if (this.uniqueFields == null) {
					this.uniqueFields = new HashSet<FieldSet>();
				}
				this.uniqueFields.addAll(uniqueFieldSets);
			}
		}
	}
	
	// ------------------------------------------------------------------------
	// Access of stub annotations
	// ------------------------------------------------------------------------
	
	/**
	 * Returns the key columns for the specific input, if all keys are preserved
	 * by this node. Null, otherwise.
	 * 
	 * @param input
	 * @return
	 */
	protected int[] getConstantKeySet(int input) {
		Operator<?> contract = getPactContract();
		if (contract instanceof AbstractUdfOperator<?, ?>) {
			AbstractUdfOperator<?, ?> abstractPact = (AbstractUdfOperator<?, ?>) contract;
			int[] keyColumns = abstractPact.getKeyColumns(input);
			if (keyColumns != null) {
				if (keyColumns.length == 0) {
					return null;
				}
				for (int keyColumn : keyColumns) {
					if (!isFieldConstant(input, keyColumn)) {
						return null;	
					}
				}
				return keyColumns;
			}
		}
		return null;
	}
	
	/**
	 * An optional method where nodes can describe which fields will be unique in their output.
	 * @return
	 */
	public List<FieldSet> createUniqueFieldsForNode() {
		return null;
	}
	
	/**
	 * Gets the FieldSets which are unique in the output of the node. 
	 * 
	 * @return
	 */
	public Set<FieldSet> getUniqueFields() {
		return this.uniqueFields == null ? Collections.<FieldSet>emptySet() : this.uniqueFields;
	}
	
	// --------------------------------------------------------------------------------------------
	//                                    Pruning
	// --------------------------------------------------------------------------------------------
	
	protected void prunePlanAlternatives(List<PlanNode> plans) {
		if (plans.isEmpty()) {
			throw new CompilerException("No plan meeting the requirements could be created @ " + this + ". Most likely reason: Too restrictive plan hints.");
		}
		// shortcut for the simple case
		if (plans.size() == 1) {
			return;
		}
		
		// we can only compare plan candidates that made equal choices
		// at the branching points. for each choice at a branching point,
		// we need to keep the cheapest (wrt. interesting properties).
		// if we do not keep candidates for each branch choice, we might not
		// find branch compatible candidates when joining the branches back.
		
		// for pruning, we are quasi AFTER the node, so in the presence of
		// branches, we need form the per-branch-choice groups by the choice
		// they made at the latest unjoined branching node. Note that this is
		// different from the check for branch compatibility of candidates, as
		// this happens on the input sub-plans and hence BEFORE the node (therefore
		// it is relevant to find the latest (partially) joined branch point.
		
		if (this.openBranches == null || this.openBranches.isEmpty()) {
			prunePlanAlternativesWithCommonBranching(plans);
		} else {
			// partition the candidates into groups that made the same sub-plan candidate
			// choice at the latest unclosed branch point
			
			final OptimizerNode[] branchDeterminers = new OptimizerNode[this.openBranches.size()];
			
			for (int i = 0; i < branchDeterminers.length; i++) {
				branchDeterminers[i] = this.openBranches.get(this.openBranches.size() - 1 - i).getBranchingNode();
			}
			
			// this sorter sorts by the candidate choice at the branch point
			Comparator<PlanNode> sorter = new Comparator<PlanNode>() {
				
				@Override
				public int compare(PlanNode o1, PlanNode o2) {
					for (int i = 0; i < branchDeterminers.length; i++) {
						PlanNode n1 = o1.getCandidateAtBranchPoint(branchDeterminers[i]);
						PlanNode n2 = o2.getCandidateAtBranchPoint(branchDeterminers[i]);
						int hash1 = System.identityHashCode(n1);
						int hash2 = System.identityHashCode(n2);
						
						if (hash1 != hash2) {
							return hash1 - hash2;
						}
					}
					return 0;
				}
			};
			Collections.sort(plans, sorter);
			
			List<PlanNode> result = new ArrayList<PlanNode>();
			List<PlanNode> turn = new ArrayList<PlanNode>();
			
			final PlanNode[] determinerChoice = new PlanNode[branchDeterminers.length];

			while (!plans.isEmpty()) {
				// take one as the determiner
				turn.clear();
				PlanNode determiner = plans.remove(plans.size() - 1);
				turn.add(determiner);
				
				for (int i = 0; i < determinerChoice.length; i++) {
					determinerChoice[i] = determiner.getCandidateAtBranchPoint(branchDeterminers[i]);
				}

				// go backwards through the plans and find all that are equal
				boolean stillEqual = true;
				for (int k = plans.size() - 1; k >= 0 && stillEqual; k--) {
					PlanNode toCheck = plans.get(k);
					
					for (int i = 0; i < branchDeterminers.length; i++) {
						PlanNode checkerChoice = toCheck.getCandidateAtBranchPoint(branchDeterminers[i]);
					
						if (checkerChoice != determinerChoice[i]) {
							// not the same anymore
							stillEqual = false;
							break;
						}
					}
					
					if (stillEqual) {
						// the same
						plans.remove(k);
						turn.add(toCheck);
					}
				}

				// now that we have only plans with the same branch alternatives, prune!
				if (turn.size() > 1) {
					prunePlanAlternativesWithCommonBranching(turn);
				}
				result.addAll(turn);
			}

			// after all turns are complete
			plans.clear();
			plans.addAll(result);
		}
	}
	
	protected void prunePlanAlternativesWithCommonBranching(List<PlanNode> plans) {
		// for each interesting property, which plans are cheapest
		final RequestedGlobalProperties[] gps = (RequestedGlobalProperties[]) this.intProps.getGlobalProperties().toArray(new RequestedGlobalProperties[this.intProps.getGlobalProperties().size()]);
		final RequestedLocalProperties[] lps = (RequestedLocalProperties[]) this.intProps.getLocalProperties().toArray(new RequestedLocalProperties[this.intProps.getLocalProperties().size()]);
		
		final PlanNode[][] toKeep = new PlanNode[gps.length][];
		final PlanNode[] cheapestForGlobal = new PlanNode[gps.length];
		
		
		PlanNode cheapest = null; // the overall cheapest plan

		// go over all plans from the list
		for (PlanNode candidate : plans) {
			// check if that plan is the overall cheapest
			if (cheapest == null || (cheapest.getCumulativeCosts().compareTo(candidate.getCumulativeCosts()) > 0)) {
				cheapest = candidate;
			}

			// find the interesting global properties that this plan matches
			for (int i = 0; i < gps.length; i++) {
				if (gps[i].isMetBy(candidate.getGlobalProperties())) {
					// the candidate meets the global property requirements. That means
					// it has a chance that its local properties are re-used (they would be
					// destroyed if global properties need to be established)
					
					if (cheapestForGlobal[i] == null || (cheapestForGlobal[i].getCumulativeCosts().compareTo(candidate.getCumulativeCosts()) > 0)) {
						cheapestForGlobal[i] = candidate;
					}
					
					final PlanNode[] localMatches;
					if (toKeep[i] == null) {
						localMatches = new PlanNode[lps.length];
						toKeep[i] = localMatches;
					} else {
						localMatches = toKeep[i];
					}
					
					for (int k = 0; k < lps.length; k++) {
						if (lps[k].isMetBy(candidate.getLocalProperties())) {
							final PlanNode previous = localMatches[k];
							if (previous == null || previous.getCumulativeCosts().compareTo(candidate.getCumulativeCosts()) > 0) {
								// this one is cheaper!
								localMatches[k] = candidate;
							}
						}
					}
				}
			}
		}

		// all plans are set now
		plans.clear();

		// add the cheapest plan
		if (cheapest != null) {
			plans.add(cheapest);
			cheapest.setPruningMarker(); // remember that that plan is in the set
		}
		
		// skip the top down delta cost check for now (TODO: implement this)
		// add all others, which are optimal for some interesting properties
		for (int i = 0; i < gps.length; i++) {
			if (toKeep[i] != null) {
				final PlanNode[] localMatches = toKeep[i];
				for (int k = 0; k < localMatches.length; k++) {
					final PlanNode n = localMatches[k];
					if (n != null && !n.isPruneMarkerSet()) {
						n.setPruningMarker();
						plans.add(n);
					}
				}
			}
			if (cheapestForGlobal[i] != null) {
				final PlanNode n = cheapestForGlobal[i];
				if (!n.isPruneMarkerSet()) {
					n.setPruningMarker();
					plans.add(n);
				}
			}
		}
	}
	
	
	// --------------------------------------------------------------------------------------------
	//                       Handling of branches
	// --------------------------------------------------------------------------------------------

	public boolean hasUnclosedBranches() {
		return this.openBranches != null && !this.openBranches.isEmpty();
	}

	public Set<OptimizerNode> getClosedBranchingNodes() {
		return this.closedBranchingNodes;
	}
	
	public List<UnclosedBranchDescriptor> getOpenBranches() {
		return this.openBranches;
	}

	/**
	 * @param toParent
	 * @return
	 */
	protected List<UnclosedBranchDescriptor> getBranchesForParent(PactConnection toParent) {
		if (this.outgoingConnections.size() == 1) {
			// return our own stack of open branches, because nothing is added
			if (this.openBranches == null || this.openBranches.isEmpty()) {
				return Collections.emptyList();
			} else {
				return new ArrayList<UnclosedBranchDescriptor>(this.openBranches);
			}
		}
		else if (this.outgoingConnections.size() > 1) {
			// we branch add a branch info to the stack
			List<UnclosedBranchDescriptor> branches = new ArrayList<UnclosedBranchDescriptor>(4);
			if (this.openBranches != null) {
				branches.addAll(this.openBranches);
			}

			// find out, which output number the connection to the parent
			int num;
			for (num = 0; num < this.outgoingConnections.size(); num++) {
				if (this.outgoingConnections.get(num) == toParent) {
					break;
				}
			}
			if (num >= this.outgoingConnections.size()) {
				throw new CompilerException("Error in compiler: "
					+ "Parent to get branch info for is not contained in the outgoing connections.");
			}

			// create the description and add it
			long bitvector = 0x1L << num;
			branches.add(new UnclosedBranchDescriptor(this, bitvector));
			return branches;
		}
		else {
			throw new CompilerException(
				"Error in compiler: Cannot get branch info for successor in a node with no successors.");
		}
	}

	
	protected void removeClosedBranches(List<UnclosedBranchDescriptor> openList) {
		if (openList == null || openList.isEmpty() || this.closedBranchingNodes == null || this.closedBranchingNodes.isEmpty()) {
			return;
		}
		
		Iterator<UnclosedBranchDescriptor> it = openList.iterator();
		while (it.hasNext()) {
			if (this.closedBranchingNodes.contains(it.next().getBranchingNode())) {
				//this branch was already closed --> remove it from the list
				it.remove();
			}
		}
	}
	
	protected void addClosedBranches(Set<OptimizerNode> alreadyClosed) {
		if (alreadyClosed == null || alreadyClosed.isEmpty()) {
			return;
		}
		
		if (this.closedBranchingNodes == null) { 
			this.closedBranchingNodes = new HashSet<OptimizerNode>(alreadyClosed);
		} else {
			this.closedBranchingNodes.addAll(alreadyClosed);
		}
	}
	
	protected void addClosedBranch(OptimizerNode alreadyClosed) {
		if (this.closedBranchingNodes == null) { 
			this.closedBranchingNodes = new HashSet<OptimizerNode>();
		}
		this.closedBranchingNodes.add(alreadyClosed);
	}
	
	/**
	 * Checks whether to candidate plans for the sub-plan of this node are comparable. The two
	 * alternative plans are comparable, if
	 * 
	 * a) There is no branch in the sub-plan of this node
	 * b) Both candidates have the same candidate as the child at the last open branch. 
	 * 
	 * @param subPlan1
	 * @param subPlan2
	 * @return True if the nodes are branch compatible in the inputs.
	 */
	protected boolean areBranchCompatible(PlanNode plan1, PlanNode plan2) {
		if (plan1 == null || plan2 == null) {
			throw new NullPointerException();
		}
		
		// if there is no open branch, the children are always compatible.
		// in most plans, that will be the dominant case
		if (this.hereJoinedBranches == null || this.hereJoinedBranches.isEmpty()) {
			return true;
		}

		for (OptimizerNode joinedBrancher : hereJoinedBranches) {
			final PlanNode branch1Cand = plan1.getCandidateAtBranchPoint(joinedBrancher);
			final PlanNode branch2Cand = plan2.getCandidateAtBranchPoint(joinedBrancher);
			
			if (branch1Cand != null && branch2Cand != null && branch1Cand != branch2Cand) {
				return false;
			}
		}
		return true;
	}
	
	/**
	 * The node IDs are assigned in graph-traversal order (pre-order), hence, each list is sorted by ID in ascending order and
	 * all consecutive lists start with IDs in ascending order.
	 */
	protected final boolean mergeLists(List<UnclosedBranchDescriptor> child1open, List<UnclosedBranchDescriptor> child2open, List<UnclosedBranchDescriptor> result) {

		//remove branches which have already been closed
		removeClosedBranches(child1open);
		removeClosedBranches(child2open);
		
		result.clear();
		
		// check how many open branches we have. the cases:
		// 1) if both are null or empty, the result is null
		// 2) if one side is null (or empty), the result is the other side.
		// 3) both are set, then we need to merge.
		if (child1open == null || child1open.isEmpty()) {
			if(child2open != null && !child2open.isEmpty()) {
				result.addAll(child2open);
			}
			return false;
		}
		
		if (child2open == null || child2open.isEmpty()) {
			result.addAll(child1open);
			return false;
		}

		int index1 = child1open.size() - 1;
		int index2 = child2open.size() - 1;
		
		boolean didCloseABranch = false;

		// as both lists (child1open and child2open) are sorted in ascending ID order
		// we can do a merge-join-like loop which preserved the order in the result list
		// and eliminates duplicates
		while (index1 >= 0 || index2 >= 0) {
			int id1 = -1;
			int id2 = index2 >= 0 ? child2open.get(index2).getBranchingNode().getId() : -1;

			while (index1 >= 0 && (id1 = child1open.get(index1).getBranchingNode().getId()) > id2) {
				result.add(child1open.get(index1));
				index1--;
			}
			while (index2 >= 0 && (id2 = child2open.get(index2).getBranchingNode().getId()) > id1) {
				result.add(child2open.get(index2));
				index2--;
			}

			// match: they share a common branching child
			if (id1 == id2) {
				didCloseABranch = true;
				
				// if this is the latest common child, remember it
				OptimizerNode currBanchingNode = child1open.get(index1).getBranchingNode();
				
				long vector1 = child1open.get(index1).getJoinedPathsVector();
				long vector2 = child2open.get(index2).getJoinedPathsVector();
				
				// check if this is the same descriptor, (meaning that it contains the same paths)
				// if it is the same, add it only once, otherwise process the join of the paths
				if (vector1 == vector2) {
					result.add(child1open.get(index1));
				} else {
					if (this.hereJoinedBranches == null) {
						this.hereJoinedBranches = new ArrayList<OptimizerNode>(2);
					}
					this.hereJoinedBranches.add(currBanchingNode);

					// see, if this node closes the branch
					long joinedInputs = vector1 | vector2;

					// this is 2^size - 1, which is all bits set at positions 0..size-1
					long allInputs = (0x1L << currBanchingNode.getOutgoingConnections().size()) - 1;

					if (joinedInputs == allInputs) {
						// closed - we can remove it from the stack
						addClosedBranch(currBanchingNode);
					} else {
						// not quite closed
						result.add(new UnclosedBranchDescriptor(currBanchingNode, joinedInputs));
					}
				}

				index1--;
				index2--;
			}
		}

		// merged. now we need to reverse the list, because we added the elements in reverse order
		Collections.reverse(result);
		return didCloseABranch;
	}
	
	/**
	 *
	 */
	public static final class UnclosedBranchDescriptor
	{
		protected OptimizerNode branchingNode;

		protected long joinedPathsVector;

		/**
		 * @param branchingNode
		 * @param joinedPathsVector
		 */
		protected UnclosedBranchDescriptor(OptimizerNode branchingNode, long joinedPathsVector)
		{
			this.branchingNode = branchingNode;
			this.joinedPathsVector = joinedPathsVector;
		}

		public OptimizerNode getBranchingNode() {
			return this.branchingNode;
		}

		public long getJoinedPathsVector() {
			return this.joinedPathsVector;
		}
		
		@Override
		public String toString() {
			return "(" + this.branchingNode.getPactContract() + ") [" + this.joinedPathsVector + "]";
		}
	}

	@Override
	public OptimizerNode getOptimizerNode() {
		return this;
	}
	
	@Override
	public PlanNode getPlanNode() {
		return null;
	}
	
	@Override
	public Iterable<DumpableConnection<OptimizerNode>> getDumpableInputs() {
		List<DumpableConnection<OptimizerNode>> allInputs = new ArrayList<DumpableConnection<OptimizerNode>>();
		
		allInputs.addAll(getIncomingConnections());
		allInputs.addAll(getBroadcastConnections());
		
		return allInputs;
	}
	
	@Override
	public String toString() {
		StringBuilder bld = new StringBuilder();

		bld.append(getName());
		bld.append(" (").append(getPactContract().getName()).append(") ");

		int i = 1; 
		for (PactConnection conn : getIncomingConnections()) {
			bld.append('(').append(i++).append(":").append(conn.getShipStrategy() == null ? "null" : conn.getShipStrategy().name()).append(')');
		}

		return bld.toString();
	}

	public int[] getRemappedKeys(int input) {
		return this.remappedKeys[input];
	}
}
