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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import eu.stratosphere.pact.common.contract.AbstractPact;
import eu.stratosphere.pact.common.contract.CompilerHints;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.plan.Visitable;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.Costs;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;
import eu.stratosphere.pact.compiler.util.PactType;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

/**
 * This class represents a node in the optimizer's internal representation of the PACT plan. It contains
 * extra information about estimates, hints and data properties.
 * 
 * @author Stephan Ewen
 * @author Fabian Hueske
 */
public abstract class OptimizerNode implements Visitable<OptimizerNode>, EstimateProvider
{
	// ------------------------------------------------------------------------
	//                              Members
	// ------------------------------------------------------------------------

	private final Contract pactContract; // The contract (Reduce / Match / DataSource / ...)
	
	private List<PactConnection> outgoingConnections; // The links to succeeding nodes

	private List<InterestingProperties> intProps; // the interesting properties of this node

	protected List<UnclosedBranchDescriptor> openBranches; // stack of branches in the sub-graph that are not joined
	
	protected Set<OptimizerNode> closedBranchingNodes; // stack of branching nodes which have already been closed

	protected LocalStrategy localStrategy; // The local strategy, if it is fixed by a hint
	
	protected Map<FieldSet, Long> estimatedCardinality = new HashMap<FieldSet, Long>(); // the estimated number of distinct keys in the output
	
	protected Set<FieldSet> uniqueFields = new HashSet<FieldSet>(); // set of attributes that will always be unique after this node

	protected long estimatedOutputSize = -1; // the estimated size of the output (bytes)

	protected long estimatedNumRecords = -1; // the estimated number of key/value pairs in the output

	protected int stubOutCardLB; // The lower bound of the stubs output cardinality
	
	protected int stubOutCardUB; // The upper bound of the stubs output cardinality

	private int degreeOfParallelism = -1; // the number of parallel instances of this node

	protected int instancesPerMachine = -1; // the number of parallel instance that will run on the same machine
	
	private long minimalGuaranteedMemory;

	protected int id = -1; // the id for this node.

	// ------------------------------------------------------------------------
	//                      Constructor / Setup
	// ------------------------------------------------------------------------

	/**
	 * Creates a new node for the optimizer plan.
	 * 
	 * @param pactContract The PACT that the node represents.
	 */
	public OptimizerNode(Contract pactContract)
	{
		if (pactContract == null) {
			throw new NullPointerException("The contract must not ne null.");
		}
		this.pactContract = pactContract;
		readStubAnnotations();
	}

//	/**
//	 * This is an internal copy-constructor that is used to create copies of the nodes
//	 * for plan enumeration. This constructor copies all properties (deep, if objects)
//	 * except the outgoing connections and the costs. The connections are omitted,
//	 * because the copied nodes are used in different enumerated trees/graphs. The
//	 * costs are only computed for the actual alternative plan, so they are not
//	 * copied themselves.
//	 * 
//	 * @param toClone
//	 *        The node to clone.
//	 * @param globalProps
//	 *        The global properties of this copy.
//	 * @param localProps
//	 *        The local properties of this copy.
//	 */
//	protected OptimizerNode(OptimizerNode toClone, GlobalProperties globalProps, LocalProperties localProps) {
//		this.pactContract = toClone.pactContract;
//		this.localStrategy = toClone.localStrategy;
//
//		this.localProps = localProps;
//		this.globalProps = globalProps;
//
//		this.estimatedOutputSize = toClone.estimatedOutputSize;
//		
//		this.estimatedCardinality.putAll(toClone.estimatedCardinality);
//		this.estimatedNumRecords = toClone.estimatedNumRecords;
//
//		this.stubOutCardLB = toClone.stubOutCardLB;
//		this.stubOutCardUB = toClone.stubOutCardUB;
//		
//		this.id = toClone.id;
//		this.degreeOfParallelism = toClone.degreeOfParallelism;
//		this.instancesPerMachine = toClone.instancesPerMachine;
//		
//		if (toClone.uniqueFields != null && toClone.uniqueFields.size() > 0) {
//			for (FieldSet uniqueField : toClone.uniqueFields) {
//				this.uniqueFields.add((FieldSet)uniqueField.clone());
//			}
//		}
//		
//		// check, if this node branches. if yes, this candidate must be associated with
//		// the branching template node.
//		if (toClone.isBranching()) {
//			this.branchPlan = new HashMap<OptimizerNode, OptimizerNode>(6);
//			this.branchPlan.put(toClone, this);
//		}
//
//		// remember the highest node in our sub-plan that branched.
//		this.lastJoinedBranchNode = toClone.lastJoinedBranchNode;
//	}

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
	public abstract List<PactConnection> getIncomingConnections();


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
	public abstract List<PlanNode> getAlternativePlans(CostEstimator estimator);

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
	
	/**
	 * Reads all constant stub annotations. Constant stub annotations are defined per input.
	 */
	protected abstract void readConstantAnnotation();
	
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
	
//	/**
//	 * Gets a copy of local properties from this OptimizedNode for the parent node.
//	 * If the parent node has a different DoP all local properties are lost.
//	 * 
//	 * @return The local properties.
//	 */
//	public LocalProperties getLocalPropertiesForParent(OptimizerNode parent) {
//		LocalProperties localPropsForParent = this.localProps.createCopy();
//		if (this.degreeOfParallelism != parent.getDegreeOfParallelism()) {
//			localPropsForParent.reset();
//		}
//		return localPropsForParent;
//	}
//
//	/**
//	 * Gets a copy of global properties from this OptimizedNode for the parent node.
//	 * If the parent node has a different DoP ordering is lost and partitioning is at 
//	 * most PartitionProperty.ANY
//	 * 
//	 * @return The global properties.
//	 */
//	public GlobalProperties getGlobalPropertiesForParent(OptimizerNode parent) {
//		GlobalProperties globalPropsForParent = this.globalProps.createCopy();
//		if (this.degreeOfParallelism != parent.getDegreeOfParallelism()) {
//			globalPropsForParent.setOrdering(null);
//			if (globalPropsForParent.getPartitioning() != PartitionProperty.NONE) {
//				globalPropsForParent.setPartitioning(PartitionProperty.ANY, globalPropsForParent.getPartitionedFields());
//			}
//		}
//		return globalPropsForParent;
//	}

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
		Long estimate = estimatedCardinality.get(cP);
		return estimate == null ? -1L : estimate.longValue();
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
	public void computeUnionOfInterestingPropertiesFromSuccessors()
	{
		List<PactConnection> conns = getOutgoingConnections();
		if (conns.size() == 0) {
			// no incoming, we have none ourselves
			this.intProps = Collections.<InterestingProperties>emptyList();
		} else if (conns.size() == 1) {
			// one incoming, no need to make a union, just take them
			List<InterestingProperties> ips = conns.get(0).getInterestingProperties();
			this.intProps = ips.isEmpty() ?
				Collections.<InterestingProperties>emptyList() :
				new ArrayList<InterestingProperties>(ips);
		} else {
			// union them
			List<InterestingProperties> props = null;
			for (PactConnection conn : conns) {
				List<InterestingProperties> ips = conn.getInterestingProperties();
				if (ips.size() > 0) {
					if (props == null) {
						props = new ArrayList<InterestingProperties>();
					}
					InterestingProperties.mergeUnionOfInterestingProperties(props, ips);
				}
			}
			this.intProps = (props == null || props.isEmpty()) ? Collections.<InterestingProperties>emptyList() : props;
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
	public void computeOutputEstimates(DataStatistics statistics)
	{
		// sanity checking
		for (PactConnection c : getIncomingConnections()) {
			if (c.getSourcePact() == null) {
				throw new CompilerException("Bug: Estimate computation called before inputs have been set.");
			}
		}
		
		// 
		CompilerHints hints = getPactContract().getCompilerHints();

		computeUniqueFields();
		
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
				if(hints.getAvgRecordsEmittedPerStubCall() > 1.0) {
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
			if (this.openBranches == null)
				return null;
			else
				return new ArrayList<UnclosedBranchDescriptor>(this.openBranches);
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

	
	protected void removeClosedBranches(List<UnclosedBranchDescriptor> openList) {
		if (openList == null || openList.isEmpty() || this.closedBranchingNodes == null || this.closedBranchingNodes.isEmpty())
			return;
		
		Iterator<UnclosedBranchDescriptor> it = openList.iterator();
		while (it.hasNext()) {
			if (this.closedBranchingNodes.contains(it.next().getBranchingNode())) {
				//this branch was already closed --> remove it from the list
				it.remove();
			}
		}
	}
	
	protected void addClosedBranches(Set<OptimizerNode> alreadyClosed) {
		if (alreadyClosed == null || alreadyClosed.isEmpty()) 
			return;
		if (this.closedBranchingNodes == null) 
			this.closedBranchingNodes = new HashSet<OptimizerNode>(alreadyClosed);
		else 
			this.closedBranchingNodes.addAll(alreadyClosed);
	}
	
	protected void addClosedBranch(OptimizerNode alreadyClosed) {
		if (this.closedBranchingNodes == null) 
			this.closedBranchingNodes = new HashSet<OptimizerNode>();
		this.closedBranchingNodes.add(alreadyClosed);
	}
	/*
	 * node IDs are assigned in graph-traversal order (pre-order)
	 * hence, each list is sorted by ID in ascending order and all consecutive lists start with IDs in ascending order
	 */
	protected List<UnclosedBranchDescriptor> mergeLists(List<UnclosedBranchDescriptor> child1open, List<UnclosedBranchDescriptor> child2open) {

		//remove branches which have already been closed
		removeClosedBranches(child1open);
		removeClosedBranches(child2open);
		
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

//				if (this.lastJoinedBranchNode == null) {
//					this.lastJoinedBranchNode = currBanchingNode;
//				}

				// see, if this node closes the branch
				long joinedInputs = child1open.get(index1).getJoinedPathsVector()
					| child2open.get(index2).getJoinedPathsVector();

				// this is 2^size - 1, which is all bits set at positions 0..size-1
				long allInputs = (0x1L << currBanchingNode.getOutgoingConnections().size()) - 1;

				if (joinedInputs == allInputs) {
					// closed - we can remove it from the stack
					addClosedBranch(currBanchingNode);
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
	 * Reads all stub annotations, i.e. which fields remain constant, what cardinality bounds the
	 * functions have, which fields remain unique.
	 */
	protected void readStubAnnotations() {
		readConstantAnnotation();
		readOutputCardBoundAnnotation();
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
	
	
	protected void readUniqueFieldsAnnotation() {
		if (pactContract.getCompilerHints() != null) {
			Set<FieldSet> uniqueFieldSets = pactContract.getCompilerHints().getUniqueFields();
			if (uniqueFieldSets != null) {
				this.uniqueFields.addAll(uniqueFieldSets);
			}
		}
	}
	
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
	 * Returns the key columns for the specific input, if all keys are preserved
	 * by this node. Null, otherwise.
	 * 
	 * @param input
	 * @return
	 */
	protected int[] getConstantKeySet(int input)
	{
		Contract contract = getPactContract();
		if (contract instanceof AbstractPact<?>) {
			AbstractPact<?> abstractPact = (AbstractPact<?>) contract;
			int[] keyColumns = abstractPact.getKeyColumnNumbers(input);
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
	 * 
	 */
	private void computeUniqueFields()
	{
		if (this.stubOutCardUB > 1 || this.stubOutCardUB < 0) {
			return;
		}
		
		//check which uniqueness properties are created by this node
		List<FieldSet> uniqueFields = createUniqueFieldsForNode();
		if (uniqueFields != null ) {
			this.uniqueFields.addAll(uniqueFields);
		}
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
		return this.uniqueFields;
	}
	
	
//	/**
//	 * Checks whether the FieldSet is unique in the input of the node
//	 * 
//	 * @param fieldSet
//	 * @param input
//	 * @return
//	 */
//	public boolean isFieldSetUnique(FieldSet fieldSet, int input) {
//
//		if (fieldSet == null || fieldSet.size() == 0) {
//			return true;
//		}
//		
//		for (FieldSet uniqueField : this.getUniqueFieldsForInput(input)) {
//			if (fieldSet.containsAll(uniqueField)) {
//				return true;
//			}
//		}
//		return false;
//	}
	
	// --------------------------------------------------------------------------------------------
	
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

		int i = 1; 
		for(PactConnection conn : getIncomingConnections()) {
			bld.append('(').append(i++).append(":").append(conn.getShipStrategy() == null ? "null" : conn.getShipStrategy().name()).append(')');
		}

		return bld.toString();
	}
	
	// --------------------------------------------------------------------------------------------
	//                                Branching and Pruning
	// --------------------------------------------------------------------------------------------
	
	/**
	 *
	 */
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
	
	protected void prunePlanAlternatives(List<PlanNode> plans)
	{
		// for each interesting property, which plans are cheapest
		final PlanNode[] toKeep = new PlanNode[this.intProps.size()]; 
		PlanNode cheapest = null; // the overall cheapest plan

		// go over all plans from the list
		for (PlanNode candidate : plans) {
			// check if that plan is the overall cheapest
			if (cheapest == null || (cheapest.getCumulativeCosts().compareTo(candidate.getCumulativeCosts()) > 0)) {
				cheapest = candidate;
			}

			// find the interesting properties that this plan matches
			for (int i = 0; i < this.intProps.size(); i++) {
				if (this.intProps.get(i).isMetBy(candidate)) {
					final PlanNode previous = toKeep[i];
					// the candidate meets them. if it is the first one to meet the interesting properties,
					// or the previous one was more expensive, keep it
					if (previous == null || previous.getCumulativeCosts().compareTo(candidate.getCumulativeCosts()) > 0) {
						toKeep[i] = candidate;
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
		final Costs cheapestCosts = cheapest.getCumulativeCosts();

		// add all others, which are optimal for some interesting properties
		for (int i = 0; i < toKeep.length; i++) {
			final PlanNode n = toKeep[i];
			if (n != null && !n.isPruneMarkerSet()) {
				final Costs maxDelta = this.intProps.get(i).getMaximalCosts();
				if (!cheapestCosts.isOtherMoreThanDeltaAbove(n.getCumulativeCosts(), maxDelta)) {
					n.setPruningMarker();
					plans.add(n);
				}
			}
		}
	}
}
