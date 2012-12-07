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

package eu.stratosphere.pact.compiler.dataproperties;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import eu.stratosphere.pact.compiler.costs.Costs;
import eu.stratosphere.pact.compiler.plan.OptimizerNode;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;

/**
 * The interesting properties that a node in the optimizer plan hands to its predecessors. It has the
 * purpose to tell the preceding nodes, which data properties might have the advantage, because they would
 * let the node fulfill its pact cheaper. More on optimization with interesting properties can be found
 * in the works on the volcano- and cascades optimizer framework.
 */
public class InterestingProperties implements Cloneable
{
	private Costs maximalCostsGlobal; // the maximal costs that it may take to establish the global properties
	
	private Costs maximalCostsLocal; // the maximal costs that it may take to establish the local properties

	private Set<RequestedGlobalProperties> globalProps; // the global properties, i.e. properties across partitions

	private Set<RequestedLocalProperties> localProps; // the local properties, i.e. properties within partitions

	// ------------------------------------------------------------------------

	public InterestingProperties() {
		this.maximalCostsGlobal = new Costs(0, 0);
		this.maximalCostsLocal = new Costs(0, 0);

		this.globalProps = new HashSet<RequestedGlobalProperties>();
		this.localProps = new HashSet<RequestedLocalProperties>();
	}

	/**
	 * Private constructor for cloning purposes.
	 * 
	 * @param maxCostsGlobal The maximal costs for the global properties.
	 * @param maxCostsLocal The maximal costs for the local properties.
	 * @param globalProps  The global properties for this new object.
	 * @param localProps The local properties for this new object.
	 */
	private InterestingProperties(Costs maxCostsGlobal, Costs maxCostsLocal, 
			Set<RequestedGlobalProperties> globalProps, Set<RequestedLocalProperties> localProps)
	{
		this.maximalCostsGlobal = maxCostsGlobal;
		this.maximalCostsLocal = maxCostsLocal;
		
		this.globalProps = globalProps;
		this.localProps = localProps;
	}

	// ------------------------------------------------------------------------

	public void addGlobalProperties(RequestedGlobalProperties props, Costs maxCosts) {
		this.globalProps.add(props);
		// keep the larger costs
		if (this.maximalCostsGlobal.compareTo(maxCosts) < 0) {
			this.maximalCostsGlobal = maxCosts;
		}
	}
	
	public void addLocalProperties(RequestedLocalProperties props, Costs maxCosts) {
		this.localProps.add(props);
		// keep the larger costs
		if (this.maximalCostsLocal.compareTo(maxCosts) < 0) {
			this.maximalCostsLocal = maxCosts;
		}
	}
	
	public void addInterestingProperties(InterestingProperties other) {
		this.globalProps.addAll(other.globalProps);
		this.localProps.addAll(other.localProps);
		
		if (this.maximalCostsGlobal.compareTo(other.maximalCostsGlobal) > 0) {
			this.maximalCostsGlobal = other.maximalCostsGlobal;
		}
		if (this.maximalCostsLocal.compareTo(other.maximalCostsLocal) > 0) {
			this.maximalCostsLocal = other.maximalCostsLocal;
		}
	}
	
	/**
	 * Gets the maximal costs that that it may take to establish the global properties, before
	 * they become worthless.
	 * 
	 * @return The maximal costs to establish the global properties.
	 */
	public Costs getMaximalCostsGlobalProperties() {
		return this.maximalCostsGlobal;
	}
	
	/**
	 * Gets the maximal costs that that it may take to establish the local properties, before
	 * they become worthless.
	 * 
	 * @return The maximal costs to establish the global properties.
	 */
	public Costs getMaximalCostsLocalProperties() {
		return this.maximalCostsLocal;
	}

	/**
	 * Gets the interesting local properties.
	 * 
	 * @return The interesting local properties.
	 */
	public Set<RequestedLocalProperties> getLocalProperties() {
		return this.localProps;
	}

	/**
	 * Gets the interesting global properties.
	 * 
	 * @return The interesting global properties.
	 */
	public Set<RequestedGlobalProperties> getGlobalProperties() {
		return this.globalProps;
	}

	/**
	 * Checks, if the global interesting properties are met by the given node.
	 * 
	 * @param node The node to test.
	 * @return True, if the node meets any of the global properties, false otherwise.
	 */
	public boolean isMetGlobally(PlanNode node) {
		for (RequestedGlobalProperties rgp : this.globalProps) {
			if (rgp.isMetBy(node.getGlobalProperties())) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Checks, if the local interesting properties are met by the given node.
	 * 
	 * @param node The node to test.
	 * @return True, if the node meets any of the local properties, false otherwise.
	 */
	public boolean isMetLocally(PlanNode node) {
		for (RequestedLocalProperties rlp : this.localProps) {
			if (rlp.isMetBy(node.getLocalProperties())) {
				return true;
			}
		}
		return false;
	}
	
	public InterestingProperties filterByCodeAnnotations(OptimizerNode node, int input)
	{
		InterestingProperties iProps = new InterestingProperties();
		
		for (RequestedGlobalProperties rgp : this.globalProps) {
			RequestedGlobalProperties filtered = rgp.filterByNodesConstantSet(node, input);
			if (filtered != null && !filtered.isTrivial()) {
				iProps.addGlobalProperties(filtered, maximalCostsGlobal);
			}
		}
		for (RequestedLocalProperties rlp : this.localProps) {
			RequestedLocalProperties filtered = rlp.filterByNodesConstantSet(node, input);
			if (filtered != null && !filtered.isTrivial()) {
				iProps.addLocalProperties(filtered, maximalCostsLocal);
			}
		}
		return iProps;
	}
	
	public void dropTrivials() {
		for (Iterator<RequestedGlobalProperties> iter = this.globalProps.iterator(); iter.hasNext();) {
			RequestedGlobalProperties gp = iter.next();
			if (gp.isTrivial()) {
				iter.remove();
				break;
			}
		}
		
		for (Iterator<RequestedLocalProperties> iter = this.localProps.iterator(); iter.hasNext();) {
			RequestedLocalProperties lp = iter.next();
			if (lp.isTrivial()) {
				iter.remove();
				break;
			}
		}
	}

	// ------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((globalProps == null) ? 0 : globalProps.hashCode());
		result = prime * result + ((localProps == null) ? 0 : localProps.hashCode());
		result = prime * result + ((maximalCostsGlobal == null) ? 0 : maximalCostsGlobal.hashCode());
		result = prime * result + ((maximalCostsLocal == null) ? 0 : maximalCostsLocal.hashCode());
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj != null && obj instanceof InterestingProperties) {
			InterestingProperties other = (InterestingProperties) obj;
			return this.globalProps.equals(other.globalProps) &&
					this.localProps.equals(other.localProps) &&
					this.maximalCostsGlobal.equals(other.maximalCostsGlobal) &&
					this.maximalCostsLocal.equals(other.maximalCostsLocal);
		} else {
			return false;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "InterestingProperties [ maximalGlobalCosts=" + maximalCostsGlobal + 
				", maximalLocalCosts=" + maximalCostsLocal+ 
				", globalProps=" + globalProps + 
				", localProps=" + localProps + " ]";
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public InterestingProperties clone() {
		HashSet<RequestedGlobalProperties> globalProps = new HashSet<RequestedGlobalProperties>();
		for (RequestedGlobalProperties p : this.globalProps) {
			globalProps.add(p.clone());
		}
		HashSet<RequestedLocalProperties> localProps = new HashSet<RequestedLocalProperties>();
		for (RequestedLocalProperties p : this.localProps) {
			localProps.add(p.clone());
		}
		
		return new InterestingProperties(this.maximalCostsGlobal.clone(), this.maximalCostsLocal.clone(),
			globalProps, localProps);
	}
}
