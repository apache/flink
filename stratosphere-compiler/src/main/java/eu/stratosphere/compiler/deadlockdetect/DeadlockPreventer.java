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

package eu.stratosphere.compiler.deadlockdetect;

import java.util.ArrayList;
import java.util.List;
import eu.stratosphere.compiler.plan.BulkIterationPlanNode;
import eu.stratosphere.compiler.plan.DualInputPlanNode;
import eu.stratosphere.compiler.plan.PlanNode;
import eu.stratosphere.compiler.plan.SingleInputPlanNode;
import eu.stratosphere.compiler.plan.WorksetIterationPlanNode;
import eu.stratosphere.pact.runtime.task.DamBehavior;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.util.Visitor;

/**
 * 	Certain pipelined flows may lead to deadlocks, in which case we need to make sure the pipelines are broken or made elastic enough to prevent that.
 *
 *  This is only relevat to pipelined data flows where one operator has more than one consumers (successors in the flow).
 *	
 *	Most cases are caught by the general logic that deals with branching/joining flows. The following cases need additional checks:
 *
 *                    <build>
 *  (source1) ------ (join)
 *	          \    /    <probe>
 *	           \  /
 *	            \/
 *	            /\
 *	           /  \
 *	          /    \    <probe>
 *	(source2) ------(join)
 *	                    <build>
 *	
 *	Since both sources pipeline their data into a build and a probe side, they get stalled by the back pressure from the probe side (which waits for the build side to complete) and never finish the build side.
 *	
 *	We can model dependencies of pipelined / materialized connections and do a reguar cyclic dependencies check to detect such situations. Pipelined connections have a dependency from sender to receiver, non-pipelined (fully dammed) connections have a dependency from receiver to sender.
 *
 */
public class DeadlockPreventer implements Visitor<PlanNode> {
	
	private DeadlockGraph g;
	
	public DeadlockPreventer() {
		this.g = new DeadlockGraph();
	}

	public void resolveDeadlocks(List<? extends PlanNode> sinks) {

		for(PlanNode s : sinks) {
			s.accept(this);
		}
		if(g.hasCycle()) {
			
			// in the remaining plan is a cycle
			for(DeadlockVertex v : g.vertices) {

				// first strategy to fix -> swap build and probe side
				if(v.getOriginal().getDriverStrategy().equals(DriverStrategy.HYBRIDHASH_BUILD_FIRST)) {

					v.getOriginal().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND);
					
					if(hasDeadlock(sinks)) {
						// Didn't fix anything -> revert
						v.getOriginal().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_FIRST);
					}
					else {
						// deadlock resolved
						break;
					}
				}
				
				// other direction
				if(v.getOriginal().getDriverStrategy().equals(DriverStrategy.HYBRIDHASH_BUILD_SECOND)) {
					
					v.getOriginal().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_FIRST);
					
					if(hasDeadlock(sinks)) {
						// Didn't fix anything -> revert
						v.getOriginal().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND);
					}
					else {
						// deadlock resolved
						break;
					}
				}
			}
			
			// switching build and probe side did not help -> pipeline breaker
			for(DeadlockVertex v : g.vertices) {
				if(v.getOriginal() instanceof DualInputPlanNode) {
					DualInputPlanNode n = (DualInputPlanNode) v.getOriginal();
					
					// what is the pipelined side? (other side should be a dam, otherwise operator could not be source of deadlock)
					if(!(n.getDriverStrategy().firstDam().equals(DamBehavior.FULL_DAM) || n.getInput1().getLocalStrategy().dams() || n.getInput1().getTempMode().breaksPipeline())
							&& (n.getDriverStrategy().secondDam().equals(DamBehavior.FULL_DAM) || n.getInput2().getLocalStrategy().dams() || n.getInput2().getTempMode().breaksPipeline())) {
						n.getInput1().setTempMode(n.getInput1().getTempMode().makePipelineBreaker());
					}
					else if( !(n.getDriverStrategy().secondDam().equals(DamBehavior.FULL_DAM) || n.getInput2().getLocalStrategy().dams() || n.getInput2().getTempMode().breaksPipeline())
							&& (n.getDriverStrategy().firstDam().equals(DamBehavior.FULL_DAM) || n.getInput1().getLocalStrategy().dams() || n.getInput1().getTempMode().breaksPipeline())) {
						n.getInput2().setTempMode(n.getInput2().getTempMode().makePipelineBreaker());
					}
					
					// Deadlock resolved?
					if(!hasDeadlock(sinks)) {
						break;
					}
				}
			}
		}
	
	}
	
	/**
	 * Creates new DeadlockGraph from plan and checks for cycles
	 * 
	 * @param plan
	 * @return
	 */
	public boolean hasDeadlock(List<? extends PlanNode> sinks) {
		this.g = new DeadlockGraph();
		
		for(PlanNode s : sinks) {
			s.accept(this);
		}
		
		if(g.hasCycle())
			return true;
		else
			return false;
	}

	/**
	 * 
	 * @param visitable
	 * @return
	 */
	@Override
	public boolean preVisit(PlanNode visitable) {
		
		g.addVertex(visitable);
		return true;
	}

	@Override
	public void postVisit(PlanNode visitable) {
		if(visitable instanceof SingleInputPlanNode) {
			SingleInputPlanNode n = (SingleInputPlanNode) visitable;
			
			if(n.getDriverStrategy().firstDam().equals(DamBehavior.FULL_DAM) || n.getInput().getLocalStrategy().dams() || n.getInput().getTempMode().breaksPipeline()) {
				g.addEdge(n, n.getPredecessor());
			}
			else {
				g.addEdge(n.getPredecessor(), n);
			}
		}
		else if(visitable instanceof DualInputPlanNode) {
			DualInputPlanNode n = (DualInputPlanNode) visitable;
			
			if(n.getDriverStrategy().firstDam().equals(DamBehavior.FULL_DAM) || n.getInput1().getLocalStrategy().dams() || n.getInput1().getTempMode().breaksPipeline()) {
				g.addEdge(n, n.getInput1().getSource());
			}
			else {
				g.addEdge(n.getInput1().getSource(), n);
			}
			
			if(!n.getDriverStrategy().equals(DriverStrategy.NONE) && (n.getDriverStrategy().secondDam().equals(DamBehavior.FULL_DAM) || n.getInput2().getLocalStrategy().dams() || n.getInput2().getTempMode().breaksPipeline())) {
				g.addEdge(n, n.getInput2().getSource());
			}
			else {
				g.addEdge(n.getInput2().getSource(), n);
			}
		}
		
		
		// recursively fix iterations
		if (visitable instanceof BulkIterationPlanNode) {
			
			DeadlockPreventer dp = new DeadlockPreventer();
			List<PlanNode> planSinks = new ArrayList<PlanNode>(1);
			
			BulkIterationPlanNode pspn = (BulkIterationPlanNode) visitable;
			planSinks.add(pspn.getRootOfStepFunction());
			
			dp.resolveDeadlocks(planSinks);
			
		}
		else if (visitable instanceof WorksetIterationPlanNode) {
			
			DeadlockPreventer dp = new DeadlockPreventer();
			List<PlanNode> planSinks = new ArrayList<PlanNode>(2);
			
			WorksetIterationPlanNode pspn = (WorksetIterationPlanNode) visitable;
			planSinks.add(pspn.getSolutionSetDeltaPlanNode());
			planSinks.add(pspn.getNextWorkSetPlanNode());
			
			dp.resolveDeadlocks(planSinks);
			
		}
	}
}