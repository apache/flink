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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.CompilerHints;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.DualInputContract;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ExplicitCopiesFirst;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ExplicitCopiesSecond;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ExplicitProjectionsFirst;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ExplicitProjectionsSecond;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ImplicitOperationFirst;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ImplicitOperationSecond;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ReadsFirst;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ReadsSecond;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ImplicitOperation.ImplicitOperationMode;
import eu.stratosphere.pact.common.util.FieldList;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.Costs;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

/**
 * A node in the optimizer plan that represents a PACT with a two different inputs, such as MATCH or CROSS.
 * The two inputs are not substitutable in their sides.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public abstract class TwoInputNode extends OptimizerNode
{
	private List<OptimizerNode> cachedPlans; // a cache for the computed alternative plans

	protected PactConnection input1 = null; // The first input edge

	protected PactConnection input2 = null; // The second input edge

	protected FieldList keySet1; // The set of key fields for the first input (order is relevant!)
	
	protected FieldList keySet2; // The set of key fields for the second input (order is relevant!)
	
	// ------------- Stub Annotations
	
	protected FieldSet reads1; // set of fields that are read by the stub
	
	protected FieldSet explProjections1; // set of fields that are explicitly projected from the first input
	
	protected FieldSet explCopies1; // set of fields that are copied from the first input to output 
	
	protected ImplicitOperationMode implOpMode1; // implicit operation of the stub on the first input
	
	protected FieldSet reads2; // set of fields that are read by the stub
	
	protected FieldSet explProjections2; // set of fields that are explicitly projected from the second input
	
	protected FieldSet explCopies2; // set of fields that are copied from the second input to output 
	
	protected ImplicitOperationMode implOpMode2; // implicit operation of the stub on the second input
	
	/**
	 * Creates a new node with a single input for the optimizer plan.
	 * 
	 * @param pactContract
	 *        The PACT that the node represents.
	 */
	public TwoInputNode(DualInputContract<?> pactContract) {
		super(pactContract);

		this.keySet1 = new FieldList(pactContract.getKeyColumnNumbers(0));
		this.keySet2 = new FieldList(pactContract.getKeyColumnNumbers(1));
		
	}

	/**
	 * Copy constructor to create a copy of a node with different predecessors. The predecessors
	 * is assumed to be of the same type as in the template node and merely copies with different
	 * strategies, as they are created in the process of the plan enumeration.
	 * 
	 * @param template
	 *        The node to create a copy of.
	 * @param pred1
	 *        The new predecessor for the first input.
	 * @param pred2
	 *        The new predecessor for the second input.
	 * @param conn1
	 *        The old connection of the first input to copy properties from.
	 * @param conn2
	 *        The old connection of the second input to copy properties from.
	 * @param globalProps
	 *        The global properties of this copy.
	 * @param localProps
	 *        The local properties of this copy.
	 */
	protected TwoInputNode(TwoInputNode template, OptimizerNode pred1, OptimizerNode pred2, PactConnection conn1,
			PactConnection conn2, GlobalProperties globalProps, LocalProperties localProps)
	{
		super(template, globalProps, localProps);
		
		this.reads1 = template.reads1;
		this.reads2 = template.reads2;
		this.explCopies1 = template.explCopies1;
		this.explCopies2 = template.explCopies2;
		this.explProjections1 = template.explProjections1;
		this.explProjections2 = template.explProjections2;
		this.implOpMode1 = template.implOpMode1;
		this.implOpMode2 = template.implOpMode2;
		this.keySet1 = template.keySet1;
		this.keySet2 = template.keySet2;

		if(pred1 != null) {
			this.input1 = new PactConnection(conn1, pred1, this);
		}
		
		if(pred2 != null) {
			this.input2 = new PactConnection(conn2, pred2, this);
		}

		// merge the branchPlan maps according the the template's uncloseBranchesStack
		if (template.openBranches != null)
		{
			if (this.branchPlan == null) {
				this.branchPlan = new HashMap<OptimizerNode, OptimizerNode>(8);
			}

			for (UnclosedBranchDescriptor uc : template.openBranches) {
				OptimizerNode brancher = uc.branchingNode;
				OptimizerNode selectedCandidate = null;

				if(pred1 != null) {
					if(pred1.branchPlan != null) {
						// predecessor 1 has branching children, see if it got the branch we are looking for
						selectedCandidate = pred1.branchPlan.get(brancher);
						this.branchPlan.put(brancher, selectedCandidate);
					}
				}
				
				if(selectedCandidate == null && pred2 != null) {
					if(pred2.branchPlan != null) {
						// predecessor 2 has branching children, see if it got the branch we are looking for
						selectedCandidate = pred2.branchPlan.get(brancher);
						this.branchPlan.put(brancher, selectedCandidate);
					}
				}

				if (selectedCandidate == null) {
					throw new CompilerException(
						"Candidates for a node with open branches are missing information about the selected candidate ");
				}
			}
		}
	}

	// ------------------------------------------------------------------------
	

	/**
	 * Gets the <tt>PactConnection</tt> through which this node receives its <i>first</i> input.
	 * 
	 * @return The first input connection.
	 */
	public PactConnection getFirstInConn() {
		return this.input1;
	}

	/**
	 * Gets the <tt>PactConnection</tt> through which this node receives its <i>second</i> input.
	 * 
	 * @return The second input connection.
	 */
	public PactConnection getSecondInConn() {
		return this.input2;
	}

	/**
	 * Sets the <tt>PactConnection</tt> through which this node receives its <i>first</i> input.
	 * 
	 * @param conn
	 *        The first input connection.
	 */
	public void setFirstInConn(PactConnection conn) {
		this.input1 = conn;
	}

	/**
	 * Sets the <tt>PactConnection</tt> through which this node receives its <i>second</i> input.
	 * 
	 * @param conn
	 *        The second input connection.
	 */
	public void setSecondInConn(PactConnection conn) {
		this.input2 = conn;
	}
	
	/**
	 * TODO
	 */
	public OptimizerNode getFirstPredNode() {
		if(this.input1 != null)
			return this.input1.getSourcePact();
		else
			return null;
	}
	
	/**
	 * TODO
	 */
	public OptimizerNode getSecondPredNode() {
		if(this.input2 != null)
			return this.input2.getSourcePact();
		else
			return null;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getIncomingConnections()
	 */
	@Override
	public List<PactConnection> getIncomingConnections() {
		ArrayList<PactConnection> inputs = new ArrayList<PactConnection>(2);
		inputs.add(input1);
		inputs.add(input2);
		return inputs;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setInputs(java.util.Map)
	 */
	@Override
	public void setInputs(Map<Contract, OptimizerNode> contractToNode) {
		// get the predecessors
		DualInputContract<?> contr = (DualInputContract<?>) getPactContract();
		
		List<Contract> leftPreds = contr.getFirstInputs();
		List<Contract> rightPreds = contr.getSecondInputs();
		
		for(Contract cl : leftPreds) {
			OptimizerNode pred1 = contractToNode.get(cl);
			// create the connections and add them
			PactConnection conn1 = new PactConnection(pred1, this);
			this.input1 = conn1;
			pred1.addOutConn(conn1);
		}

		for(Contract cr : rightPreds) {
			OptimizerNode pred2 = contractToNode.get(cr);
			// create the connections and add them
			PactConnection conn2 = new PactConnection(pred2, this);
			this.input2 = conn2;
			pred2.addOutConn(conn2);
		}

		// see if there is a hint that dictates which shipping strategy to use for BOTH inputs
		Configuration conf = getPactContract().getParameters();
		String shipStrategy = conf.getString(PactCompiler.HINT_SHIP_STRATEGY, null);
		if (shipStrategy != null) {
			if (PactCompiler.HINT_SHIP_STRATEGY_FORWARD.equals(shipStrategy)) {
				this.input1.setShipStrategy(ShipStrategy.FORWARD);
				this.input2.setShipStrategy(ShipStrategy.FORWARD);
			} else if (PactCompiler.HINT_SHIP_STRATEGY_BROADCAST.equals(shipStrategy)) {
				this.input1.setShipStrategy(ShipStrategy.BROADCAST);
				this.input2.setShipStrategy(ShipStrategy.BROADCAST);
			} else if (PactCompiler.HINT_SHIP_STRATEGY_REPARTITION.equals(shipStrategy)) {
				this.input1.setShipStrategy(ShipStrategy.PARTITION_HASH);
				this.input2.setShipStrategy(ShipStrategy.PARTITION_HASH);
			} else {
				throw new CompilerException("Unknown hint for shipping strategy: " + shipStrategy);
			}
		}

		// see if there is a hint that dictates which shipping strategy to use for the FIRST input
		shipStrategy = conf.getString(PactCompiler.HINT_SHIP_STRATEGY_FIRST_INPUT, null);
		if (shipStrategy != null) {
			if (PactCompiler.HINT_SHIP_STRATEGY_FORWARD.equals(shipStrategy)) {
				this.input1.setShipStrategy(ShipStrategy.FORWARD);
			} else if (PactCompiler.HINT_SHIP_STRATEGY_BROADCAST.equals(shipStrategy)) {
				this.input1.setShipStrategy(ShipStrategy.BROADCAST);
			} else if (PactCompiler.HINT_SHIP_STRATEGY_REPARTITION.equals(shipStrategy)) {
				this.input1.setShipStrategy(ShipStrategy.PARTITION_HASH);
			} else {
				throw new CompilerException("Unknown hint for shipping strategy of input one: " + shipStrategy);
			}
		}

		// see if there is a hint that dictates which shipping strategy to use for the SECOND input
		shipStrategy = conf.getString(PactCompiler.HINT_SHIP_STRATEGY_SECOND_INPUT, null);
		if (shipStrategy != null) {
			if (PactCompiler.HINT_SHIP_STRATEGY_FORWARD.equals(shipStrategy)) {
				this.input2.setShipStrategy(ShipStrategy.FORWARD);
			} else if (PactCompiler.HINT_SHIP_STRATEGY_BROADCAST.equals(shipStrategy)) {
				this.input2.setShipStrategy(ShipStrategy.BROADCAST);
			} else if (PactCompiler.HINT_SHIP_STRATEGY_REPARTITION.equals(shipStrategy)) {
				this.input2.setShipStrategy(ShipStrategy.PARTITION_HASH);
			} else {
				throw new CompilerException("Unknown hint for shipping strategy of input two: " + shipStrategy);
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getAlternativePlans()
	 */
	@Override
	final public List<OptimizerNode> getAlternativePlans(CostEstimator estimator) {
		// check if we have a cached version
		if (this.cachedPlans != null) {
			return this.cachedPlans;
		}

		// step down to all producer nodes for first input and calculate alternative plans
		List<? extends OptimizerNode> subPlans1 = this.getFirstPredNode().getAlternativePlans(estimator);
		
		// step down to all producer nodes for second input and calculate alternative plans
		List<? extends OptimizerNode> subPlans2 = this.getSecondPredNode().getAlternativePlans(estimator);

		List<OptimizerNode> outputPlans = new ArrayList<OptimizerNode>();
		computeValidPlanAlternatives(subPlans1, subPlans2, estimator,  outputPlans);
		
		// prune the plans
		prunePlanAlternatives(outputPlans);

		// cache the result only if we have multiple outputs --> this function gets invoked multiple times
		if (this.getOutConns() != null && this.getOutConns().size() > 1) {
			this.cachedPlans = outputPlans;
		}

		return outputPlans;
	}
	
	/**
	 * Takes a list with all sub-plan-combinations (each is a list by itself) and produces alternative
	 * plans for the current node using the single sub-plans-combinations.
	 *  
	 * @param altSubPlans1	 	All subplans of the first input
	 * @param altSubPlans2	 	All subplans of the second input
	 * @param estimator			Cost estimator to be used
	 * @param outputPlans		The generated output plans
	 */
	protected abstract void computeValidPlanAlternatives(List<? extends OptimizerNode> altSubPlans1,
			List<? extends OptimizerNode> altSubPlans2, CostEstimator estimator, List<OptimizerNode> outputPlans);
		
	/**
	 * Checks if the subPlan has a valid outputSize estimation.
	 * 
	 * @param subPlan		the subPlan to check
	 * 
	 * @return	{@code true} if all values are valid, {@code false} otherwise
	 */
	protected boolean haveValidOutputEstimates(OptimizerNode subPlan) {
	
		if(subPlan.getEstimatedOutputSize() == -1)
			return false;
		else
			return true;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeUnclosedBranchStack()
	 */
	@Override
	public void computeUnclosedBranchStack() {
		if (this.openBranches != null) {
			return;
		}

		List<UnclosedBranchDescriptor> result1 = new ArrayList<UnclosedBranchDescriptor>();
		// TODO: check if merge is really necessary
		result1 = mergeLists(result1, this.getFirstPredNode().getBranchesForParent(this));
		
		List<UnclosedBranchDescriptor> result2 = new ArrayList<UnclosedBranchDescriptor>();
		// TODO: check if merge is really necessary
		result2 = mergeLists(result2, this.getSecondPredNode().getBranchesForParent(this));

		this.openBranches = mergeLists(result1, result2);
	}

	// ------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.compiler.plan.OptimizerNode#accept(eu.stratosphere.pact.common.plan.Visitor
	 * )
	 */
	@Override
	public void accept(Visitor<OptimizerNode> visitor) {
		boolean descend = visitor.preVisit(this);

		if (descend) {
			if (this.getFirstPredNode() != null) {
				this.getFirstPredNode().accept(visitor);
			}
			if (this.getSecondPredNode() != null) {
				this.getSecondPredNode().accept(visitor);
			}

			visitor.postVisit(this);
		}
	}

	/**
	 * This function overrides the standard behavior of computing costs in the {@link eu.stratosphere.pact.compiler.plan.OptimizerNode}.
	 * Since nodes with multiple inputs may join branched plans, care must be taken not to double-count the costs of the subtree rooted
	 * at the last unjoined branch.
	 * 
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setCosts(eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void setCosts(Costs nodeCosts) {
		super.setCosts(nodeCosts);
		
		// check, if this node has no branch beneath it, no double-counted cost then
		if (this.lastJoinedBranchNode == null) {
			return;
		}

		// TODO: Check this!
		// get the cumulative costs of the last joined branching node
		OptimizerNode lastCommonChild = this.getFirstPredNode().branchPlan.get(this.lastJoinedBranchNode);
		Costs douleCounted = lastCommonChild.getCumulativeCosts();
		getCumulativeCosts().subtractCosts(douleCounted);
		
	}
	
	// ---------------------- Stub Annotation Handling
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#readReadsAnnotation()
	 */
	@Override
	protected void readReadsAnnotation() {
		DualInputContract<?> c = (DualInputContract<?>)super.getPactContract();
		
		// get readSet annotation from stub
		ReadsFirst readSet1Annotation = c.getUserCodeClass().getAnnotation(ReadsFirst.class);
		ReadsSecond readSet2Annotation = c.getUserCodeClass().getAnnotation(ReadsSecond.class);
		
		// extract readSets from annotations
		if(readSet1Annotation == null) {
			this.reads1 = null;
		} else {
			this.reads1 = new FieldSet(readSet1Annotation.fields());
		}
		
		if(readSet2Annotation == null) {
			this.reads2 = null;
		} else {
			this.reads2 = new FieldSet(readSet2Annotation.fields());
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#readCopyProjectionAnnotations()
	 */
	@Override
	protected void readCopyProjectionAnnotations() {
		DualInputContract<?> c = (DualInputContract<?>)super.getPactContract();
		
		// get updateSet annotation from stub
		ImplicitOperationFirst implOp1Annotation = c.getUserCodeClass().getAnnotation(ImplicitOperationFirst.class);
		ImplicitOperationSecond implOp2Annotation = c.getUserCodeClass().getAnnotation(ImplicitOperationSecond.class);
		
		// set sets to null by default
		this.implOpMode1 = null;
		this.explCopies1 = null;
		this.explProjections1 = null;
		
		if(implOp1Annotation != null) {
			switch(implOp1Annotation.implicitOperation()) {
			case Copy:
				// implicit copy -> we have explicit projection
				ExplicitProjectionsFirst explProjAnnotation = c.getUserCodeClass().getAnnotation(ExplicitProjectionsFirst.class);
				if(explProjAnnotation != null) {
					this.implOpMode1 = ImplicitOperationMode.Copy;
					this.explProjections1 = new FieldSet(explProjAnnotation.fields());
				}
				break;
			case Projection:
				// implicit projection -> we have explicit copies
				ExplicitCopiesFirst explCopyjAnnotation = c.getUserCodeClass().getAnnotation(ExplicitCopiesFirst.class);
				if(explCopyjAnnotation != null) {
					this.implOpMode1 = ImplicitOperationMode.Projection;
					this.explCopies1 = new FieldSet(explCopyjAnnotation.fields());
				}
				break;
			}
		}
		
		// set sets to null by default
		this.implOpMode2 = null;
		this.explCopies2 = null;
		this.explProjections2 = null;

		if(implOp2Annotation != null) {
			switch(implOp2Annotation.implicitOperation()) {
			case Copy:
				// implicit copy -> we have explicit projection
				ExplicitProjectionsSecond explProjAnnotation = c.getUserCodeClass().getAnnotation(ExplicitProjectionsSecond.class);
				if(explProjAnnotation != null) {
					this.implOpMode2 = ImplicitOperationMode.Copy;
					this.explProjections2 = new FieldSet(explProjAnnotation.fields());
				}
				break;
			case Projection:
				// implicit projection -> we have explicit copies
				ExplicitCopiesSecond explCopyjAnnotation = c.getUserCodeClass().getAnnotation(ExplicitCopiesSecond.class);
				if(explCopyjAnnotation != null) {
					this.implOpMode2 = ImplicitOperationMode.Projection;
					this.explCopies2 = new FieldSet(explCopyjAnnotation.fields());
				}
				break;
			}
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeOutputSchema(java.util.List)
	 */
	@Override
	public FieldSet computeOutputSchema(List<FieldSet> inputSchemas) {

		if(inputSchemas.size() != 2)
			throw new IllegalArgumentException("TwoInputNode requires exactly 2 input nodes");
		
		// fields that are kept constant from the inputs
		FieldSet constFields1 = null;
		FieldSet constFields2 = null;
		
		// explicit writes must be defined
		if(explWrites == null) {
			return null;
		}
		
		if(implOpMode1 == null) {
			constFields1 = null;
		} else {
			
			switch(implOpMode1) {
			case Copy:
				// implicit copy -> we keep everything, except for explicit projections
				if(this.explProjections1 != null) {
					constFields1 = new FieldSet(inputSchemas.get(0));
					constFields1.removeAll(this.explProjections1);
					
				} else {
					constFields1 = null;
				}
				break;
			case Projection:
				// implicit projection -> we keep only explicit copies
				constFields1 = this.explCopies1;
				break;
			}
		}
		
		if(implOpMode2 == null) {
			constFields2 = null;
		} else {
			
			switch(implOpMode2) {
			case Copy:
				// implicit copy -> we keep everything, except for explicit projections
				if(this.explProjections2 != null) {
					constFields2 = new FieldSet(inputSchemas.get(1));
					constFields2.removeAll(this.explProjections2);
				} else {
					constFields2 = null;
				}
				break;
			case Projection:
				// implicit projection -> we keep only explicit copies
				constFields2 = this.explCopies2;
				break;
			}
		}
		
		if(constFields1 != null && constFields2 != null) {
			// output schema are kept fields plus explicit writes
			return new FieldSet(constFields1, new FieldSet(constFields2, this.explWrites));
		} else {
			return null;
		}
		
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#deriveOutputSchema()
	 */
	@Override
	public void deriveOutputSchema() {
		
		// collect input schema of node
		List<FieldSet> inputSchemas = new ArrayList<FieldSet>(2);
		inputSchemas.add(this.getFirstPredNode().getOutputSchema());
		inputSchemas.add(this.getSecondPredNode().getOutputSchema());
		
		// compute output schema given the node's input schemas
		this.outputSchema = computeOutputSchema(inputSchemas);
		
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isValidInputSchema(int, int[])
	 */
	@Override
	public boolean isValidInputSchema(int input, FieldSet inputSchema) {
		
		if(input < 0 || input > 1)
			throw new IndexOutOfBoundsException("TwoInputNode has inputs 0 or 1");
		
		// check for first input
		if(input == 0) {
			// check that we can perform all required reads on the input schema
			if(this.reads1 != null && !inputSchema.containsAll(this.reads1))
				return false;
			// check that the input schema contains all keys
			if(this.keySet1 != null && !inputSchema.containsAll(this.keySet1))
				return false;
			// check that implicit mode is set
			if(this.implOpMode1 == null) {
				return false;
			}
			// check that explicit projections can be performed
			if(this.implOpMode1 == ImplicitOperationMode.Copy && 
					!inputSchema.containsAll(this.explProjections1))
				return false;
			// check that explicit copies can be performed
			if(this.implOpMode1 == ImplicitOperationMode.Projection &&
					!inputSchema.containsAll(this.explCopies1))
				return false;
		// check for second input
		} else {
			// check that we can perform all required reads on the input schema
			if(this.reads2 != null && !inputSchema.containsAll(this.reads2))
				return false;
			// check that the input schema contains all keys
			if(this.keySet2 != null && !inputSchema.containsAll(this.keySet2))
				return false;
			// check that implicit mode is set
			if(this.implOpMode2 == null) {
				return false;
			}
			// check that explicit projections can be performed
			if(this.implOpMode2 == ImplicitOperationMode.Copy && 
					!inputSchema.containsAll(this.explProjections2))
				return false;
			// check that explicit copies can be performed
			if(this.implOpMode2 == ImplicitOperationMode.Projection &&
					!inputSchema.containsAll(this.explCopies2))
				return false;
		}
		
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getReadSet(int)
	 */
	@Override
	public FieldSet getReadSet(int input) {

		switch(input) {
		case 0:
			return this.reads1;
		case 1:
			return this.reads2;
		case -1:
			return new FieldSet(this.reads1, this.reads2);
		default:
			throw new IndexOutOfBoundsException();
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getWriteSet(int)
	 */
	@Override
	public FieldSet getWriteSet(int input) {
		
		// get the input schemas of the node
		List<FieldSet> inputSchemas = new ArrayList<FieldSet>(2);
		inputSchemas.add(this.getFirstPredNode().getOutputSchema());
		inputSchemas.add(this.getSecondPredNode().getOutputSchema());
		
		// compute and return the write set for the node's input schemas
		return this.getWriteSet(input, inputSchemas);
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getWriteSet(int, java.util.List)
	 */
	@Override
	public FieldSet getWriteSet(int input, List<FieldSet> inputSchemas) {

		if(inputSchemas.size() != 2)
			throw new IllegalArgumentException("TwoInputNode requires exactly 2 input nodes");
		
		switch(input) {
		// compute write set for first input
		case 0:
			if(implOpMode1 != null) {
				switch(implOpMode1) {
				case Copy:
					// implicit copy -> write set are all explicit projections plus writes
					if(this.explProjections1 != null) {
						return new FieldSet(this.explProjections1, this.explWrites);
					} else {
						return null;
					}
				case Projection:
					// implicit projection -> write set are all input fields minus copied fields plus writes
					if(this.explCopies1 != null) {
						
						FieldSet writeSet = new FieldSet(inputSchemas.get(0));
						writeSet.removeAll(this.explCopies1);
						writeSet.addAll(this.explWrites);
						return writeSet;
						
					} else {
						return null;
					}
				default:
					return null;
				}
			} else {
				return null;
			}
		// compute write set for second input
		case 1:
			if(implOpMode2 != null) {
				switch(implOpMode2) {
				case Copy:
					// implicit copy -> write set are all explicit projections plus writes
					if(this.explProjections2 != null) {
						return new FieldSet(this.explProjections2, this.explWrites);
					} else {
						return null;
					}
				case Projection:
					// implicit projection -> write set are all input fields minus copied fields plus writes
					if(this.explCopies2 != null) {
						
						FieldSet writeSet = new FieldSet(inputSchemas.get(1));
						writeSet.removeAll(this.explCopies2);
						writeSet.addAll(this.explWrites);
						return writeSet;
						
					} else {
						return null;
					}
				default:
					return null;
				}
			} else {
				return null;
			}
		// compute write set for both inputs
		case -1:
			if(this.implOpMode1 != null && this.implOpMode2 != null && this.explWrites != null) {
				
				// sets of projected (and hence written) fields
				FieldSet projection1 = null;
				FieldSet projection2 = null;
				
				switch(this.implOpMode1) {
				case Copy:
					// implicit copy -> explicit projection
					projection1 = this.explProjections1;
					break;
				case Projection:
					// implicit projection -> input schema minus copied fields
					if(this.explCopies1 != null) {
						projection1 = new FieldSet(inputSchemas.get(0));
						projection1.removeAll(this.explCopies1);
					} else {
						return null;
					}
					break;
				default:
					return null;
				}
				
				switch(this.implOpMode2) {
				case Copy:
					// implicit copy -> explicit projection
					projection2 = this.explProjections2;
					break;
				case Projection:
					// implicit projection -> input schema minus copied fields
					if(this.explCopies2 != null) {
						projection2 = new FieldSet(inputSchemas.get(0));
						projection2.removeAll(this.explCopies2);
					} else {
						return null;
					}
					break;
				default:
					return null;
				}
		
				if(projection1 != null && projection2 != null) {
					// write set are projected and explicitly written fields
					return new FieldSet(projection1, new FieldSet(projection2, this.explWrites));
	
				} else {
					return null;
				}
				
			} else {
				return null;
			}
		default:
			throw new IndexOutOfBoundsException();
		}
	}
	
	/**
	 * Computes the width of output records
	 * 
	 * @return width of output records
	 */
	protected double computeAverageRecordWidth() {
		CompilerHints hints = getPactContract().getCompilerHints();

		if(hints.getAvgBytesPerRecord() != -1) {
			// use hint if available
			return hints.getAvgBytesPerRecord();
		}
	
		double avgRecordWidth = -1;
		
		if(this.getFirstPredNode() != null && 
				this.getFirstPredNode().estimatedOutputSize != -1 &&
				this.getFirstPredNode().estimatedNumRecords != -1) {
			avgRecordWidth = (this.getFirstPredNode().estimatedOutputSize / this.getFirstPredNode().estimatedNumRecords);
			
		} else {
			return -1;
		}
		
		if(this.getSecondPredNode() != null && 
				this.getSecondPredNode().estimatedOutputSize != -1 &&
				this.getSecondPredNode().estimatedNumRecords != -1) {
			
			avgRecordWidth += (this.getSecondPredNode().estimatedOutputSize / this.getSecondPredNode().estimatedNumRecords);
			
		} else {
			return -1;
		}

		return (avgRecordWidth < 1) ? 1 : avgRecordWidth;
	}

	/**
	 * Returns the key fields of the given input.
	 * 
	 * @param input The input for which key fields must be returned.
	 * @return the key fields of the given input.
	 */
	public FieldList getInputKeySet(int input) {
		switch(input) {
		case 0: return keySet1;
		case 1: return keySet2;
		default: throw new IndexOutOfBoundsException();
		}
	}
	
	public boolean isFieldKept(int input, int fieldNumber) {
		
		switch(input) {
		case 0:
			if (implOpMode1 == null) {
				return false;
			}
			switch (implOpMode1) {
			case Projection:
				return (explCopies1 == null ? false : this.explCopies1.contains(fieldNumber));
			case Copy:
				return (explProjections1 == null || explWrites == null ? false : 
					!(new FieldSet(this.explWrites, this.explProjections1)).contains(fieldNumber));
			default:
				return false;
			}
		case 1:
			if (implOpMode2 == null) {
				return false;
			}
			switch (implOpMode2) {
			case Projection:
				return (explCopies2 == null ? false : this.explCopies2.contains(fieldNumber));
			case Copy:
				return (explProjections2 == null || explWrites == null ? false : 
					!(new FieldSet(explWrites, explProjections2)).contains(fieldNumber));
			default:
				return false;
			}
		default:
			throw new IndexOutOfBoundsException();
		}
	}
	
}
