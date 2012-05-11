/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.compiler.plan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import eu.stratosphere.pact.common.contract.AbstractPact;
import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.CompilerHints;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Visitable;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ExplicitModifications;
import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.Costs;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

/**
 * This class represents a node in the internal representation of the PACT plan. The internal
 * representation is used by the optimizer to determine the algorithms to be used
 * and to create the Nephele schedule for the runtime system.
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 * @author Stephan Ewen (stephan.ewen@tu -berlin.de)
 */
public abstract class OptimizerNode implements Visitable<OptimizerNode>
{
	// ------------------------------------------------------------------------
	//                         Internal classes
	// ------------------------------------------------------------------------

	/**
	 * An enumeration describing the type of the PACT.
	 */
	public enum PactType {
		Cogroup(CoGroupContract.class),
		Cross(CrossContract.class),
		DataSource(GenericDataSource.class),
		DataSink(GenericDataSink.class),
		Map(MapContract.class),
		Match(MatchContract.class),
		Reduce(ReduceContract.class);

		private Class<? extends Contract> clazz; // The class describing the contract

		/**
		 * Private constructor to set enum attributes.
		 * 
		 * @param clazz
		 *        The class of the actual PACT contract represented by this enum constant.
		 */
		private PactType(Class<? extends Contract> clazz) {
			this.clazz = clazz;
		}

		/**
		 * Gets the class of the actual PACT contract represented by this enum constant.
		 * 
		 * @return The class of the actual PACT contract.
		 */
		public Class<? extends Contract> getPactClass() {
			return this.clazz;
		}

		/**
		 * Utility method that gets the enum constant for a PACT class.
		 * 
		 * @param pactClass
		 *        The PACT class to find the enum constant for.
		 * @return The enum constant for the given pact class.
		 */
		public static PactType getType(Class<? extends Contract> pactClass) {
			PactType[] values = PactType.values();
			for (int i = 0; i < values.length; i++) {
				if (values[i].clazz.isAssignableFrom(pactClass)) {
					return values[i];
				}
			}
			return null;
		}
	}

	// ------------------------------------------------------------------------
	//                              Members
	// ------------------------------------------------------------------------

	private final Contract pactContract; // The contract (Reduce / Match / DataSource / ...)
	
	protected int stubOutCardLB; // The lower bound of the stubs output cardinality
	
	protected int stubOutCardUB; // The upper bound of the stubs output cardinality
	
	protected FieldSet explWrites; // The set of explicitly written fields
	
	protected FieldSet outputSchema; // The fields are present in the output records
	
	private List<PactConnection> outgoingConnections; // The links to succeeding nodes

	private List<InterestingProperties> intProps; // the interesting properties of this node

	protected LocalProperties localProps; // local properties of the data produced by this node

	protected GlobalProperties globalProps; // global properties of the data produced by this node

	protected List<UnclosedBranchDescriptor> openBranches; // stack of branches in the sub-graph that are not joined
	
	protected Map<OptimizerNode, OptimizerNode> branchPlan; // the actual plan alternative chosen at a branch point

	protected OptimizerNode lastJoinedBranchNode; // the node with latest branch (node with multiple outputs)
	                                          // that both children share and that is at least partially joined

	protected LocalStrategy localStrategy; // The local strategy (sorting / hashing, ...)

	protected Costs nodeCosts; // the costs incurred by this node

	protected Costs cumulativeCosts; // the cumulative costs of all operators in the sub-tree of this node

	protected long estimatedOutputSize = -1; // the estimated size of the output (bytes)

	protected long estimatedNumRecords = -1; // the estimated number of key/value pairs in the output

	protected Map<FieldSet, Long> estimatedCardinality = new HashMap<FieldSet, Long>(); // the estimated number of distinct keys in the output
	
	protected Set<FieldSet> uniqueFields = new HashSet<FieldSet>(); // the fields which are unique

	private int degreeOfParallelism = -1; // the number of parallel instances of this node

	private int instancesPerMachine = -1; // the number of parallel instance that will run on the same machine

	private int memoryPerTask; // the amount of memory dedicated to each task, in MiBytes

	private int id = -1; // the id for this node.

	private boolean pFlag = false; // flag for the internal pruning algorithm

	// ------------------------------------------------------------------------
	//                      Constructor / Setup
	// ------------------------------------------------------------------------

	/**
	 * Creates a new node for the optimizer plan.
	 * 
	 * @param pactContract
	 *        The PACT that the node represents.
	 */
	public OptimizerNode(Contract pactContract) {
		if (pactContract == null) {
			throw new NullPointerException("The contract must not ne null.");
		}

		this.pactContract = pactContract;

		this.outgoingConnections = null;

		this.localProps = new LocalProperties();
		this.globalProps = new GlobalProperties();

		this.readStubAnnotations();
	}

	/**
	 * This is an internal copy-constructor that is used to create copies of the nodes
	 * for plan enumeration. This constructor copies all properties (deep, if objects)
	 * except the outgoing connections and the costs. The connections are omitted,
	 * because the copied nodes are used in different enumerated trees/graphs. The
	 * costs are only computed for the actual alternative plan, so they are not
	 * copied themselves.
	 * 
	 * @param toClone
	 *        The node to clone.
	 * @param globalProps
	 *        The global properties of this copy.
	 * @param localProps
	 *        The local properties of this copy.
	 */
	protected OptimizerNode(OptimizerNode toClone, GlobalProperties globalProps, LocalProperties localProps) {
		this.pactContract = toClone.pactContract;
		this.localStrategy = toClone.localStrategy;

//		this.outputContract = toClone.outputContract;

		this.localProps = localProps;
		this.globalProps = globalProps;

		this.estimatedOutputSize = toClone.estimatedOutputSize;
		
		this.estimatedCardinality.putAll(toClone.estimatedCardinality);
		this.estimatedNumRecords = toClone.estimatedNumRecords;

		this.id = toClone.id;
		this.degreeOfParallelism = toClone.degreeOfParallelism;
		this.instancesPerMachine = toClone.instancesPerMachine;
		
		this.stubOutCardLB = toClone.stubOutCardLB;
		this.stubOutCardUB = toClone.stubOutCardUB;
		
		if (toClone.uniqueFields != null && toClone.uniqueFields.size() > 0) {
			for (FieldSet uniqueField : toClone.uniqueFields) {
				this.uniqueFields.add((FieldSet)uniqueField.clone());
			}
		}
		
		this.explWrites = toClone.explWrites;
		this.outputSchema = toClone.outputSchema == null ? null : (FieldSet)toClone.outputSchema.clone(); 

		// check, if this node branches. if yes, this candidate must be associated with
		// the branching template node.
		if (toClone.isBranching()) {
			this.branchPlan = new HashMap<OptimizerNode, OptimizerNode>(6);
			this.branchPlan.put(toClone, this);
		}

		// remember the highest node in our sub-plan that branched.
		this.lastJoinedBranchNode = toClone.lastJoinedBranchNode;
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
	public abstract void setInputs(Map<Contract, OptimizerNode> contractToNode);

	/**
	 * This method needs to be overridden by subclasses to return the children.
	 * 
	 * @return The list of incoming links.
	 */
	public abstract List<List<PactConnection>> getIncomingConnections();


	/**
	 * Tells the node to compute the interesting properties for its inputs. The interesting properties
	 * for the node itself must have been computed before.
	 * The node must then see how many of interesting properties it preserves and add its own.
	 * 
	 * @param estimator		The {@code CostEstimator} instance to use for plan cost estimation. 
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
	public abstract List<? extends OptimizerNode> getAlternativePlans(CostEstimator estimator);

	/**
	 * This method implements the visit of a depth-first graph traversing visitor. Implementors must first
	 * call the <code>preVisit()</code> method, then hand the visitor to their children, and finally call
	 * the <code>postVisit()</code> method.
	 * 
	 * @param visitor
	 *        The graph traversing visitor.
	 * @see eu.stratosphere.pact.common.plan.Visitable#accept(eu.stratosphere.pact.common.plan.Visitor)
	 */
	@Override
	public abstract void accept(Visitor<OptimizerNode> visitor);

	/**
	 * Checks, whether this node requires memory for its tasks or not.
	 * 
	 * @return True, if this node contains logic that requires memory usage, false otherwise.
	 */
	public abstract int getMemoryConsumerCount();

	// ------------------------------------------------------------------------
	//                          Getters / Setters
	// ------------------------------------------------------------------------

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
	public void SetId(int id) {
		this.id = id;
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
		
		if(this.outgoingConnections == null) {
			this.outgoingConnections = new ArrayList<PactConnection>();
		}
		return this.outgoingConnections;
	}

	/**
	 * Gets the object that specifically describes the contract of this node.
	 * 
	 * @return This node's contract.
	 */
	public Contract getPactContract() {
		return this.pactContract;
	}

	/**
	 * Gets the type of the PACT as a <tt>PactType</tt> enumeration constant for this node.
	 * 
	 * @return The type of the PACT.
	 */
	public PactType getPactType() {
		return PactType.getType(this.pactContract.getClass());
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
	 * to be executed on the same machine.
	 * 
	 * @return The number of instances per machine.
	 */
	public int getInstancesPerMachine() {
		return this.instancesPerMachine;
	}

	/**
	 * Sets the number of parallel instances of the contract that are
	 * to be executed on the same machine.
	 * 
	 * @param instancesPerMachine
	 *        The instances per machine.
	 * @throws IllegalArgumentException
	 *         If the number of instances per machine is smaller than one.
	 */
	public void setInstancesPerMachine(int instancesPerMachine) {
		if (instancesPerMachine < 1) {
			throw new IllegalArgumentException();
		}
		this.instancesPerMachine = instancesPerMachine;
	}

	/**
	 * Gets the memory dedicated to each task for this node.
	 * 
	 * @return The memory per task, in MiBytes.
	 */
	public int getMemoryPerTask() {
		return this.memoryPerTask;
	}

	/**
	 * Sets the memory dedicated to each task for this node.
	 * 
	 * @param memoryPerTask
	 *        The memory per task.
	 */
	public void setMemoryPerTask(int memoryPerTask) {
		this.memoryPerTask = memoryPerTask;
	}

	/**
	 * Gets the costs incurred by this node. The costs reflect also the costs incurred by the shipping strategies
	 * of the incoming connections.
	 * 
	 * @return The node-costs, or null, if not yet set.
	 */
	public Costs getNodeCosts() {
		return this.nodeCosts;
	}

	/**
	 * Gets the cumulative costs of this nose. The cumulative costs are the the sum of the costs
	 * of this node and of all nodes in the subtree below this node.
	 * 
	 * @return The cumulative costs, or null, if not yet set.
	 */
	public Costs getCumulativeCosts() {
		return this.cumulativeCosts;
	}

	/**
	 * Gets the local strategy from this node. This determines for example for a <i>match</i> Pact whether
	 * to use a sort-merge or a hybrid hash strategy.
	 * 
	 * @return The local strategy.
	 */
	public LocalStrategy getLocalStrategy() {
		return this.localStrategy;
	}

	/**
	 * Sets the local strategy from this node. This determines the algorithms to be used to prepare the data inside a
	 * partition.
	 * 
	 * @param strategy
	 *        The local strategy to be set.
	 */
	public void setLocalStrategy(LocalStrategy strategy) {
		this.localStrategy = strategy;
	}

	/**
	 * Gets the properties that are interesting for this node to produce.
	 * 
	 * @return The interesting properties for this node, or null, if not yet computed.
	 */
	public List<InterestingProperties> getInterestingProperties() {
		return this.intProps;
	}

	/**
	 * Gets the local properties from this OptimizedNode.
	 * 
	 * @return The local properties.
	 */
	public LocalProperties getLocalProperties() {
		return this.localProps;
	}

	/**
	 * Gets the global properties from this OptimizedNode.
	 * 
	 * @return The global properties.
	 */
	public GlobalProperties getGlobalProperties() {
		return this.globalProps;
	}

	/**
	 * Gets the estimated output size from this node.
	 * 
	 * @return The estimated output size.
	 */
	public long getEstimatedOutputSize() {
		return this.estimatedOutputSize;
	}

	/**
	 * Gets the estimated number of records in the output of this node.
	 * 
	 * @return The estimated number of records.
	 */
	public long getEstimatedNumRecords() {
		return this.estimatedNumRecords;
	}

	
	public Map<FieldSet, Long> getEstimatedCardinalities() {
		return estimatedCardinality;
	}
	
	public long getEstimatedCardinality(FieldSet cP) {
		Long estimate;
		if ((estimate = estimatedCardinality.get(cP)) == null) {
			estimate = -1L;
		}
		return estimate;
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

	/**
	 * Sets the basic cost for this {@code OptimizerNode}
	 * 
	 * @param nodeCosts		The already knows costs for this node
	 * 						(this cost a produces by a concrete {@code OptimizerNode} subclass.
	 */
	public void setCosts(Costs nodeCosts) {
		// set the node costs
		this.nodeCosts = nodeCosts;

		// the cumulative costs are the node costs plus the costs of all inputs
		this.cumulativeCosts = new Costs(0, 0);
		this.cumulativeCosts.addCosts(nodeCosts);

		for(List<PactConnection> pl : getIncomingConnections()) {
			for (PactConnection p : pl) {
				Costs parentCosts = p.getSourcePact().cumulativeCosts;
				if(parentCosts != null)
					this.cumulativeCosts.addCosts(parentCosts);
			}
		}
	}

	// ------------------------------------------------------------------------
	//                              Miscellaneous
	// ------------------------------------------------------------------------

	/**
	 * This method step over all inputs recursively and combines all alternatives per input with all
	 * other alternative of all other inputs.
	 * 
	 * @param inPlans		all alternative plans for all incoming connections (which are unioned)
	 * @param predList		list of currently chosen alternative plans (has one entry for each incoming connection)
	 * 						[this list is build up recursively within the method]
	 * @param estimator		the cost estimator
	 * @param alternativeSubPlans	all generated alternative for this node
	 */
	@SuppressWarnings("unchecked")
	final protected void getAlternativeSubPlanCombinationsRecursively(List<? extends OptimizerNode>[] inPlans,
			ArrayList<OptimizerNode> predList, List<List<OptimizerNode>> alternativeSubPlans)
	{
		final int inputNumberToProcess = predList.size();
		final int numberOfAlternatives = inPlans[inputNumberToProcess].size();

		for(int i = 0; i < numberOfAlternatives; ++i) {
			predList.add(inPlans[inputNumberToProcess].get(i));
		
			// check if the hit the last recursion level
			if(inputNumberToProcess + 1 == inPlans.length) {
				// last recursion level: create a new alternative now

				// we clone the current list in order to preserve this alternative plan combination
				// otherwise we would override it later in...
				alternativeSubPlans.add((ArrayList<OptimizerNode>)predList.clone());
				
			} else {
				// step to next input and start to step though all plan alternatives
				getAlternativeSubPlanCombinationsRecursively(inPlans, predList, alternativeSubPlans);
			}
			
			// remove the added alternative plan node, in order to replace it with the next alternative at the beginning of the loop
			predList.remove(inputNumberToProcess);
		}
	}
	
	/**
	 * Checks, if all outgoing connections have their interesting properties set from their target nodes.
	 * 
	 * @return True, if on all outgoing connections, the interesting properties are set. False otherwise.
	 */
	public boolean haveAllOutputConnectionInterestingProperties() {
		if (this.outgoingConnections == null) {
			return true;
		}

		for (PactConnection conn : this.outgoingConnections) {
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
	public void computeInterestingProperties() {
		List<InterestingProperties> props = new ArrayList<InterestingProperties>();

		List<PactConnection> conns = getOutgoingConnections();
		if (conns != null) {
			for (PactConnection conn : conns) {
				List<InterestingProperties> ips = conn.getInterestingProperties();
				InterestingProperties.mergeUnionOfInterestingProperties(props, ips);
			}
		}

		this.intProps = props.isEmpty() ? Collections.<InterestingProperties> emptyList() : props;
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
		
		boolean allPredsAvailable = true;
		
		for (List<PactConnection> incomingConnections : getIncomingConnections()) {
			if (allPredsAvailable) {
				for (PactConnection incomingConnection : incomingConnections) {
					if (incomingConnection.getSourcePact() == null) {
						allPredsAvailable = false;
						break;
					}
				}	
			}
			else {
				break;
			}
		}
		
		CompilerHints hints = getPactContract().getCompilerHints();

		computeUniqueFields();
		
		// check if preceding nodes are available
		if (!allPredsAvailable) {
			// Preceding node is not available, we take hints as given
			//this.estimatedKeyCardinality = hints.getKeyCardinality();
			this.estimatedCardinality.putAll(hints.getDistinctCounts());
			
			this.estimatedNumRecords = 0;
			int count = 0;
			
			for (Entry<FieldSet, Long> cardinality : hints.getDistinctCounts().entrySet()) {
				float avgNumValues = hints.getAvgNumRecordsPerDistinctFields(cardinality.getKey());
				if (avgNumValues != -1) {
					this.estimatedNumRecords += cardinality.getValue() * avgNumValues;
					count++;
				}
			}
			
			if (count > 0) {
				this.estimatedNumRecords = (this.estimatedNumRecords /count) >= 1 ?
						(this.estimatedNumRecords /count) : 1;
			}
			else {
				this.estimatedNumRecords = -1;
			}
			
			if (this.estimatedNumRecords != -1 && hints.getAvgBytesPerRecord() != -1) {
				this.estimatedOutputSize = (this.estimatedNumRecords * hints.getAvgBytesPerRecord() >= 1) ? 
						(long) (this.estimatedNumRecords * hints.getAvgBytesPerRecord()) : 1;
			}
			
		} else {
			// We have a preceding node
		
			// ############# output cardinality estimation ##############
			
			boolean outputCardEstimated = true;
				
			this.estimatedNumRecords = 0;
			int count = 0;

			//If we have cardinalities and avg num values available for some fields, calculate 
			//the average of those
			for (Entry<FieldSet, Long> cardinality : hints.getDistinctCounts().entrySet()) {
				float avgNumValues = hints.getAvgNumRecordsPerDistinctFields(cardinality.getKey());
				if (avgNumValues != -1) {
					this.estimatedNumRecords += cardinality.getValue() * avgNumValues;
					count++;
				}
			}
			
			if (count > 0) {
				this.estimatedNumRecords = (this.estimatedNumRecords /count) >= 1 ?
						(this.estimatedNumRecords /count) : 1;
			}
			else {

				// default output cardinality is equal to number of stub calls
				this.estimatedNumRecords = this.computeNumberOfStubCalls();
				
				if(hints.getAvgRecordsEmittedPerStubCall() != -1.0 && this.computeNumberOfStubCalls() != -1) {
					// we know how many records are in average emitted per stub call
					this.estimatedNumRecords = (this.computeNumberOfStubCalls() * hints.getAvgRecordsEmittedPerStubCall() >= 1) ?
							(long) (this.computeNumberOfStubCalls() * hints.getAvgRecordsEmittedPerStubCall()) : 1;
				} else {
					outputCardEstimated = false;
				}
			}
						
			// ############# output key cardinality estimation ##########

			this.estimatedCardinality.putAll(hints.getDistinctCounts());	

			
			if (this.getUniqueFields() != null) {
				for (FieldSet uniqueFieldSet : this.uniqueFields) {
					if (this.estimatedCardinality.get(uniqueFieldSet) == null) {
						this.estimatedCardinality.put(uniqueFieldSet, this.estimatedNumRecords);
					}
				}
			}
			
			
			for (int input = 0; input < getIncomingConnections().size(); input++) {
				int[] keyColumns;
				if ((keyColumns = getConstantKeySet(input)) != null) {
					long estimatedKeyCardinality; 
					if(hints.getAvgRecordsEmittedPerStubCall() < 1.0) {
						// in average less than one record is emitted per stub call
						
						// compute the probability that at least one stub call emits a record for a given key 
						double probToKeepKey = 1.0 - Math.pow((1.0 - hints.getAvgRecordsEmittedPerStubCall()), this.computeStubCallsPerProcessedKey());

						estimatedKeyCardinality = (this.computeNumberOfProcessedKeys() * probToKeepKey >= 1) ?
								(long) (this.computeNumberOfProcessedKeys() * probToKeepKey) : 1;
					} else {
						// in average more than one record is emitted per stub call. We assume all keys are kept.
						estimatedKeyCardinality = this.computeNumberOfProcessedKeys();
					}
					
					FieldSet fieldSet = new FieldSet(keyColumns);
					if (estimatedCardinality.get(fieldSet) == null) {
						estimatedCardinality.put(fieldSet, estimatedKeyCardinality);	
					}
				}
			}
			
			if(this.estimatedNumRecords != -1) {
				for (Entry<FieldSet, Float> avgNumValues : hints.getAvgNumRecordsPerDistinctFields().entrySet()) {
					if (estimatedCardinality.get(avgNumValues.getKey()) == null) {
						long estimatedCard = (this.estimatedNumRecords / avgNumValues.getValue() >= 1) ? 
								(long) (this.estimatedNumRecords / avgNumValues.getValue()) : 1;
						estimatedCardinality.put(avgNumValues.getKey(), estimatedCard);
					}
				}
			}
			 
			// try to reversely estimate output cardinality from key cardinality
			if(!outputCardEstimated) { //this.estimatedKeyCardinality != -1 &&
				// we could derive an estimate for key cardinality but could not derive an estimate for the output cardinality
				
				long newEstimatedNumRecords = 0;
				count = 0;
				
				for (Entry<FieldSet, Long> cardinality : estimatedCardinality.entrySet()) {
					float avgNumValues = hints.getAvgNumRecordsPerDistinctFields(cardinality.getKey());
					if (avgNumValues != -1) {
						// we have a hint for average values per key
						newEstimatedNumRecords += cardinality.getValue() * avgNumValues;
						count++;
					}
				}
				
				if (count > 0) {
					newEstimatedNumRecords = (newEstimatedNumRecords /count) >= 1 ?
							(newEstimatedNumRecords /count) : 1;
				}
			}
			
				
			// ############# output size estimation #####################

			double estAvgRecordWidth = this.computeAverageRecordWidth();
			
			if(this.estimatedNumRecords != -1 && estAvgRecordWidth != -1) {
				// we have a cardinality estimate and width estimate

				this.estimatedOutputSize = (this.estimatedNumRecords * estAvgRecordWidth) >= 1 ? 
						(long)(this.estimatedNumRecords * estAvgRecordWidth) : 1;
			}
			
			// check that the key-card is maximally as large as the number of rows
			for (Entry<FieldSet, Long> cardinality : this.estimatedCardinality.entrySet()) {
				if (cardinality.getValue() > this.estimatedNumRecords) {
					cardinality.setValue(this.estimatedNumRecords);
				}
			}
		}
	}
	

	/**
	 * Takes the given list of plans that are candidates for this node in the final plan and retains for each distinct
	 * set of interesting properties only the cheapest plan.
	 * 
	 * @param plans
	 *        The plans to prune.
	 */
	public <T extends OptimizerNode> void prunePlanAlternatives(List<T> plans) {
		// shortcut for the case that there is only one plan
		if (plans.size() == 1) {
			return;
		}

		// if we have unjoined branches, split the list of plans such that only those
		// with the same candidates at the branch points are compared
		// otherwise, we may end up with the case that no compatible plans are found at
		// nodes that join
		if (this.openBranches == null) {
			prunePlansWithCommonBranchAlternatives(plans);
		} else {
			// TODO brute force still
			List<T> result = new ArrayList<T>();
			List<T> turn = new ArrayList<T>();

			while (!plans.isEmpty()) {
				turn.clear();
				T determiner = plans.remove(plans.size() - 1);
				turn.add(determiner);

				for (int k = plans.size() - 1; k >= 0; k--) {
					boolean equal = true;
					T toCheck = plans.get(k);

					for (int b = 0; b < this.openBranches.size(); b++) {
						OptimizerNode brancher = this.openBranches.get(b).branchingNode;
						OptimizerNode cand1 = determiner.branchPlan.get(brancher);
						OptimizerNode cand2 = toCheck.branchPlan.get(brancher);
						if (cand1 != cand2) {
							equal = false;
							break;
						}
					}

					if (equal) {
						turn.add(plans.remove(k));
					}
				}

				// now that we have only plans with the same branch alternatives, prune!
				if (turn.size() > 1) {
					prunePlansWithCommonBranchAlternatives(turn);
				}
				result.addAll(turn);
			}

			// after all turns are complete
			plans.clear();
			plans.addAll(result);
		}
	}

	private final <T extends OptimizerNode> void prunePlansWithCommonBranchAlternatives(List<T> plans) {
		List<List<T>> toKeep = new ArrayList<List<T>>(this.intProps.size()); // for each interesting property, which plans
		// are cheapest
		for (int i = 0; i < this.intProps.size(); i++) {
			toKeep.add(null);
		}

		T cheapest = null; // the overall cheapest plan

		// go over all plans from the list
		for (T candidate : plans) {
			// check if that plan is the overall cheapest
			if (cheapest == null || (cheapest.getCumulativeCosts().compareTo(candidate.getCumulativeCosts()) > 0)) {
				cheapest = candidate;
			}

			// find the interesting properties that this plan matches
			for (int i = 0; i < this.intProps.size(); i++) {
				if (this.intProps.get(i).isMetBy(candidate)) {
					// the candidate meets them
					if (toKeep.get(i) == null) {
						// first one to meet the interesting properties, so store it
						List<T> l = new ArrayList<T>(2);
						l.add(candidate);
						toKeep.set(i, l);
					} else {
						// others met that one before
						// see if that one is more expensive and not more general than
						// one of the others. If so, drop it.
						List<T> l = toKeep.get(i);
						boolean met = false;
						boolean replaced = false;

						for (int k = 0; k < l.size(); k++) {
							T other = l.get(k);

							// check if the candidate is both cheaper and at least as general
							if (other.getGlobalProperties().isMetBy(candidate.getGlobalProperties())
								&& other.getLocalProperties().isMetBy(candidate.getLocalProperties())
								&& other.getCumulativeCosts().compareTo(candidate.getCumulativeCosts()) > 0) {
								// replace that one with the candidate
								l.set(k, replaced ? null : candidate);
								replaced = true;
								met = true;
							} else {
								// check if the previous plan is more general and not more expensive than the candidate
								met |= (candidate.getGlobalProperties().isMetBy(other.getGlobalProperties())
									&& candidate.getLocalProperties().isMetBy(other.getLocalProperties()) && candidate
									.getCumulativeCosts().compareTo(other.getCumulativeCosts()) >= 0);
							}
						}

						if (!met) {
							l.add(candidate);
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
			cheapest.pFlag = true; // remember that that plan is in the set
		}

		Costs cheapestCosts = cheapest.cumulativeCosts;

		// add all others, which are optimal for some interesting properties
		for (int i = 0; i < toKeep.size(); i++) {
			List<T> l = toKeep.get(i);

			if (l != null) {
				Costs maxDelta = this.intProps.get(i).getMaximalCosts();

				for (T plan : l) {
					if (plan != null && !plan.pFlag) {
						plan.pFlag = true;

						// check, if that plan is not more than the delta above the costs of the
						if (!cheapestCosts.isOtherMoreThanDeltaAbove(plan.getCumulativeCosts(), maxDelta)) {
							plans.add(plan);
						}
					}
				}
			}
		}

		// reset the flags
		for (T p : plans) {
			p.pFlag = false;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder bld = new StringBuilder();

		bld.append(getName());
		bld.append(" (").append(getPactType().name()).append(") ");

		if (this.localStrategy != null) {
			bld.append('(');
			bld.append(getLocalStrategy().name());
			bld.append(") ");
		}

		List<List<PactConnection>> pl = getIncomingConnections();
		final int size = pl.size();
		for(int i = 0; i < size; ++i) {
			for (PactConnection conn : pl.get(i)) {
				bld.append('(').append(i).append(":").append(conn.getShipStrategy() == null ? "null" : conn.getShipStrategy().name()).append(')');
			}
		}

		return bld.toString();
	}

	
	/**
	 * Computes the width of output records
	 * 
	 * @return width of output records
	 */
	protected double computeAverageRecordWidth() {
		return -1;
	}
	
	/**
	 * Computes the number of stub calls.
	 * 
	 * @return the number of stub calls.
	 */
	protected long computeNumberOfStubCalls() {
		return -1;
	}
	
	
	// ------------------------------------------------------------------------
	// Handling of branches
	// ------------------------------------------------------------------------

	public boolean hasUnclosedBranches()
	{
		return this.openBranches != null && !this.openBranches.isEmpty();
	}

	protected List<UnclosedBranchDescriptor> getBranchesForParent(OptimizerNode parent)
	{
		if (this.outgoingConnections.size() == 1) {
			// return our own stack of open branches, because nothing is added
			return this.openBranches;
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
				if (this.outgoingConnections.get(num).getTargetPact() == parent) {
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
				"Error in compiler: Cannot get branch info for parent in a node woth no parents.");
		}
	}
	
	/**
	 * Checks whether to candidate plans for the sub-plan of this node are comparable. The two
	 * alternative plans are comparable, if
	 * a) There is no branch in the sub-plan of this node
	 * b) Both candidates have the same candidate as the child at the last open branch. 
	 * 
	 * @param child1Candidate
	 * @param child2Candidate
	 * @return
	 */
	protected boolean areBranchCompatible(List<OptimizerNode> child1Candidate, List<OptimizerNode> child2Candidate)
	{
		// if there is no open branch, the children are always compatible.
		// in most plans, that will be the dominant case
		if (this.lastJoinedBranchNode == null) {
			return true;
		}

		
		// the order of the branches in <joinedLists> does not matter 
		List<OptimizerNode> joinedLists;
		
		final int size1 = child1Candidate.size();
		int size = size1;
		
		if(child2Candidate != null) {
			final int size2 = child2Candidate.size();
			size += size2;
			
			joinedLists = new ArrayList<OptimizerNode>(size);
			
			for(int i = 0; i < size2; ++i) {
				joinedLists.add(child2Candidate.get(i));
			}			
		} else {
			joinedLists = new ArrayList<OptimizerNode>(size);			
		}

		for(int i = 0; i < size1; ++i) {
			joinedLists.add(child1Candidate.get(i));
		}

		// we check if each branch is compatible with all others
		for(int i = 0; i < size; ++i) {
			
			final OptimizerNode nodeToCompare = joinedLists.get(i).branchPlan.get(this.lastJoinedBranchNode);
			
			for(int j = i+1; j < size; ++j) {
				if(!(nodeToCompare == joinedLists.get(j).branchPlan.get(this.lastJoinedBranchNode))) {
					return false;
				}
			}
		}
		
		return true;
	}

	/*
	 * node IDs are assigned in graph-traversal order (pre-order)
	 * hence, each list is sorted by ID in ascending order and all consecutive lists start with IDs in ascending order
	 */
	protected List<UnclosedBranchDescriptor> mergeLists(List<UnclosedBranchDescriptor> child1open, List<UnclosedBranchDescriptor> child2open) {
		// check how many open branches we have. the cases:
		// 1) if both are null or empty, the result is null
		// 2) if one side is null (or empty), the result is the other side.
		// 3) both are set, then we need to merge.
		if(child1open == null || child1open.isEmpty()) {
			return child2open;
		}
		
		if(child2open == null || child2open.isEmpty()) {
			return child1open;
		}
		
		// both have a history. merge...
		ArrayList<UnclosedBranchDescriptor> result = new ArrayList<UnclosedBranchDescriptor>(4);


		int index1 = child1open.size() - 1;
		int index2 = child2open.size() - 1;

		// as both lists (child1open and child2open) are sorted in ascending ID order
		// we can do a merge-join-like loop which preserved the order in the result list
		// and eliminates duplicates
		while(index1 >= 0 || index2 >= 0) {
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
				// if this is the latest common child, remember it
				OptimizerNode currBanchingNode = child1open.get(index1).getBranchingNode();

				if (this.lastJoinedBranchNode == null) {
					this.lastJoinedBranchNode = currBanchingNode;
				}

				// see, if this node closes the branch
				long joinedInputs = child1open.get(index1).getJoinedPathsVector()
					| child2open.get(index2).getJoinedPathsVector();

				// this is 2^size - 1, which is all bits set at positions 0..size-1
				long allInputs = (0x1L << currBanchingNode.getOutgoingConnections().size()) - 1;

				if (joinedInputs == allInputs) {
					// closed - we can remove it from the stack
				} else {
					// not quite closed
					result.add(new UnclosedBranchDescriptor(currBanchingNode, joinedInputs));
				}

				index1--;
				index2--;
			}

		}

		// merged. now we need to reverse the list, because we added the elements in reverse order
		Collections.reverse(result);
		
		return result;
	}

	// ------------------------------------------------------------------------
	// Reading of stub annotations
	// ------------------------------------------------------------------------
	
	/**
	 * Reads all stub annotations
	 */
	private void readStubAnnotations() {
		this.readReadsAnnotation();
		this.readCopyProjectionAnnotations();
		this.readWritesAnnotation();
		this.readOutputCardBoundAnnotation();
		this.readUniqueFieldsAnnotation();
	}

	/**
	 * Reads the explicit writes stub annotation
	 */
	protected void readWritesAnnotation() {

		// get readSet annotation from stub
		ExplicitModifications addSetAnnotation = pactContract.getUserCodeClass().getAnnotation(ExplicitModifications.class);
		
		// extract addSet from annotation
		if(addSetAnnotation == null) {
			this.explWrites = null;
		} else {
			this.explWrites = new FieldSet(addSetAnnotation.fields());
		}
	}
	
	protected void readUniqueFieldsAnnotation() {
		if (pactContract.getCompilerHints() != null) {
			Set<FieldSet> uniqueFieldSets = pactContract.getCompilerHints().getUniqueFields();
			if (uniqueFieldSets != null) {
				this.uniqueFields.addAll(uniqueFieldSets);
			}
		}	
	}

	/**
	 * Reads the output cardinality stub annotations
	 */
	protected void readOutputCardBoundAnnotation() {
		
		// get readSet annotation from stub
		OutCardBounds outCardAnnotation = pactContract.getUserCodeClass().getAnnotation(OutCardBounds.class);
		
		// extract addSet from annotation
		if(outCardAnnotation == null) {
			this.stubOutCardLB = OutCardBounds.UNKNOWN;
			this.stubOutCardUB = OutCardBounds.UNKNOWN;
		} else {
			this.stubOutCardLB = outCardAnnotation.lowerBound();
			this.stubOutCardUB = outCardAnnotation.upperBound();
		}
	}
	
	/**
	 * Reads all reads stub annotations.
	 * Reads stub annotations are defined per input.
	 */
	protected abstract void readReadsAnnotation();
	
	/**
	 * Reads the copy and projection stub annotations.
	 * These annotations are defined per input.
	 */
	protected abstract void readCopyProjectionAnnotations();
	
	/**
	 * Reads and sets the output schema of the node.
	 * The schema can change if the input schema changes.
	 */
	public abstract void deriveOutputSchema();
	
	// ------------------------------------------------------------------------
	// Access of stub annotations
	// ------------------------------------------------------------------------
	
	/**
	 * Returns the lower output cardinality bound of the node.
	 * 
	 * @return the lower output cardinality bound of the node.
	 */
	public int getStubOutCardLowerBound() {
		return this.stubOutCardLB;
	}
	
	/**
	 * Returns the upper output cardinality bound of the node.
	 * 
	 * @return the upper output cardinality bound of the node.
	 */
	public int getStubOutCardUpperBound() {
		return this.stubOutCardUB;
	}
	
	/**
	 * Returns the output schema of the node.
	 * 
	 * @return the output schema of the node.
	 */
	public FieldSet getOutputSchema() {
		return this.outputSchema;
	}
	
	/**
	 * Computes the output schema of the node for given input schemas (one per input).
	 * 
	 * @param inputSchemas A list of input schemas. Element 0 of the list refers to the first input, and so on.
	 * @return The output schema of the node for the given input schemas.
	 */
	public abstract FieldSet computeOutputSchema(List<FieldSet> inputSchemas);

	/**
	 * Determines whether the node can on the given input schema for the specified input.
	 * 
	 * @param input The input for which the input schema is assumed.
	 * @param inputSchema The input schema for the specified input
	 * @return True, if the node can operate on the the schema for the specified input, false otherwise.
	 */
	public abstract boolean isValidInputSchema(int input, FieldSet inputSchema);
	
	/**
	 * Gives the read set of the node. 
	 * The read set is used to decide about reordering of nodes.
	 * 
	 * @param id of input for which the read set should be returned. 
	 *        -1 if the unioned read set over all inputs is requested. 
	 *  
	 * @return the read set for the requested input(s)
	 */
	public abstract FieldSet getReadSet(int input);
	
	/**
	 * Give the write set of the node.
	 * The write set is used to decide about reordering of nodes.
	 * 
	 * @param id of input for which the write set should be returned. 
	 *        -1 if the unioned write set over all inputs is requested. 
	 *  
	 * @return the write set for the requested input(s)
	 */
	public abstract FieldSet getWriteSet(int input);
	
	/**
	 * Give the write set of the node.
	 * 
	 * @param id of input for which the write set should be returned. 
	 *        -1 if the unioned write set over all inputs is requested. 
	 * @param inputNodes for which the write set should be computed 
	 *  
	 * @return the write set for the requested input(s)
	 */
	public abstract FieldSet getWriteSet(int input, List<FieldSet> inputSchemas);
	
	protected static final class UnclosedBranchDescriptor
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
	}
	
	public abstract boolean isFieldKept(int input, int fieldNumber);
	
	/**
	 * Computes the number of keys that are processed by the PACT.
	 * 
	 * @return the number of keys processed by the PACT.
	 */
	protected long computeNumberOfProcessedKeys() {
		return -1;
	}
	
	/**
	 * Computes the number of stub calls for one processed key. 
	 * 
	 * @return the number of stub calls for one processed key.
	 */
	protected double computeStubCallsPerProcessedKey() {
		return -1;
	}
	
	
	/**
	 * Returns the key column numbers for the specific input if it
	 * is preserved by this node. Null, otherwise.
	 * 
	 * @param input
	 * @return
	 */
	protected int[] getConstantKeySet(int input) {
		int[] keyColumns = null;
		Contract contract = getPactContract();
		if (contract instanceof AbstractPact<?>) {
			AbstractPact<?> abstractPact = (AbstractPact<?>) contract;
			keyColumns = abstractPact.getKeyColumnNumbers(input);
			if (keyColumns != null) {
				if (keyColumns.length == 0) {
					return null;
				}
				
				for (int keyColumn : keyColumns) {
					if (isFieldKept(input, keyColumn) == false) {
						return null;	
					}
				}
			}
		}
		return keyColumns;
	}
	
	public void computeUniqueFields() {
		
		if (stubOutCardUB > 1 || stubOutCardUB < 0) {
			return;
		}
		
		//check for inputs
		List<List<PactConnection>> inConnections = getIncomingConnections();
		
		for (int i = 0; i < inConnections.size(); i++) {
		
			Set<FieldSet> uniqueInChild = getUniqueFieldsForInput(i);
			
			for (FieldSet uniqueField : uniqueInChild) {
				if (keepsUniqueProperty(uniqueField, i)) {
					this.uniqueFields.add(uniqueField);
				}
			}
		}
		
		//check which uniqueness properties are created by this node
		List<FieldSet> uniqueFields = createUniqueFieldsForNode();
		if (uniqueFields != null ) {
			this.uniqueFields.addAll(uniqueFields);
		}
		
	}
	
	public boolean keepsUniqueProperty(FieldSet uniqueSet, int input) {
		for (Integer uniqueField : uniqueSet) {
			if (isFieldKept(input, uniqueField) == false) {
				return false;
			}
		}
		return true;
	}
	
	public List<FieldSet> createUniqueFieldsForNode() {
		return null;
	}
	
	/**
	 * Gets the FieldSets which are unique in the output of the node 
	 * 
	 * @return
	 */
	public Set<FieldSet> getUniqueFields() {
		return uniqueFields;
	}
	
	
	/**
	 * Checks whether the FieldSet is unique in the input of the node
	 * 
	 * @param fieldSet
	 * @param input
	 * @return
	 */
	public boolean isFieldSetUnique(FieldSet fieldSet, int input) {

		if (fieldSet == null || fieldSet.size() == 0) {
			return true;
		}
		
		for (FieldSet uniqueField : this.getUniqueFieldsForInput(input)) {
			if (fieldSet.containsAll(uniqueField)) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Get the unique FieldSets of the given input
	 */
	protected Set<FieldSet> getUniqueFieldsForInput(int input) {
		
		if (input < 0 || input >= getIncomingConnections().size()) {
			return Collections.emptySet();
		}
		
		List<PactConnection> inConnection = getIncomingConnections().get(input);
		
		if (inConnection.size() != 1) {
			return Collections.emptySet();
		}
		
		return inConnection.get(0).getSourcePact().getUniqueFields();
		
		
	}
}
