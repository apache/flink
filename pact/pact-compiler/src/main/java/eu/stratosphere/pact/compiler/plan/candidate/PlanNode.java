/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.compiler.plan.candidate;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import eu.stratosphere.pact.common.plan.Visitable;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.Costs;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
import eu.stratosphere.pact.compiler.plan.OptimizerNode;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;


/**
 *
 *
 * @author Stephan Ewen
 */
public abstract class PlanNode implements Visitable<PlanNode>
{
	protected final OptimizerNode template = null;
	
	
	protected final LocalProperties localProps; // local properties of the data produced by this node

	protected final GlobalProperties globalProps; // global properties of the data produced by this node
	
	
	private final LocalStrategy localStrategy; // The local strategy (sorting / hashing, ...)
	
	
	protected Map<OptimizerNode, PlanNode> branchPlan; // the actual plan alternative chosen at a branch point

	protected PlanNode lastJoinedBranchNode; // the node with latest branch (node with multiple outputs)
	                                          // that both children share and that is at least partially joined
	
	
	private Costs nodeCosts; // the costs incurred by this node

	private Costs cumulativeCosts; // the cumulative costs of all operators in the sub-tree of this node
	
	
	private int memoryPerTask; // the amount of memory dedicated to each task, in MiBytes
	
	protected boolean pFlag; // flag for the internal pruning algorithm
	
	// --------------------------------------------------------------------------------------------
	
	
	
	// --------------------------------------------------------------------------------------------
	//                                           Accessors
	// --------------------------------------------------------------------------------------------
	
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
	 * Gets the local strategy from this node. This determines for example for a <i>match</i> Pact whether
	 * to use a sort-merge or a hybrid hash strategy.
	 * 
	 * @return The local strategy.
	 */
	public LocalStrategy getLocalStrategy() {
		return this.localStrategy;
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
	 * Sets the basic cost for this {@code OptimizerNode}
	 * 
	 * @param nodeCosts		The already knows costs for this node
	 * 						(this cost a produces by a concrete {@code OptimizerNode} subclass.
	 */
	public void setCosts(Costs nodeCosts) {
		// set the node costs
		this.nodeCosts = nodeCosts;
		// the cumulative costs are the node costs plus the costs of all inputs
		this.cumulativeCosts = nodeCosts.clone();
		for (Iterator<PlanNode> preds = getPredecessors(); preds.hasNext();) {
			Costs parentCosts = preds.next().cumulativeCosts;
			if (parentCosts != null)
				this.cumulativeCosts.addCosts(parentCosts);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	// 
	// --------------------------------------------------------------------------------------------
	
	public abstract Iterator<PlanNode> getPredecessors();
	
	// --------------------------------------------------------------------------------------------
	//                                Branching and Pruning
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Checks whether to candidate plans for the sub-plan of this node are comparable. The two
	 * alternative plans are comparable, if
	 * a) There is no branch in the sub-plan of this node
	 * b) Both candidates have the same candidate as the child at the last open branch. 
	 * 
	 * @param subPlan1
	 * @param subPlan2
	 * @return
	 */
	protected boolean areBranchCompatible(PlanNode subPlan1, PlanNode subPlan2)
	{
		if (subPlan1 == null || subPlan2 == null)
			throw new CompilerException("SubPlans may not be null.");
		
		// if there is no open branch, the children are always compatible.
		// in most plans, that will be the dominant case
		if (this.lastJoinedBranchNode == null) {
			return true;
		}

		final PlanNode nodeToCompare = subPlan1.branchPlan.get(this.lastJoinedBranchNode);
		return nodeToCompare == subPlan2.branchPlan.get(this.lastJoinedBranchNode);
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
	
	// --------------------------------------------------------------------------------------------
}
