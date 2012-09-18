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
import java.util.List;

import eu.stratosphere.pact.compiler.Costs;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

/**
 * The interesting properties that a node in the optimizer plan hands to its predecessors. It has the
 * purpose to tell the preceding nodes, which data properties might have the advantage, because they would
 * let the node fulfill its pact cheaper. More on optimization with interesting properties can be found
 * in the works on the volcano- and cascades optimizer framework.
 */
public class InterestingProperties implements Cloneable
{
	private Costs maximalCosts; // the maximal costs that it may take to establish these
	                            // interesting properties, before they become worthless

	private GlobalProperties globalProps; // the global properties, i.e. properties across partitions

	private LocalProperties localProps; // the local properties, i.e. properties within partitions

	// ------------------------------------------------------------------------

	/**
	 * Creates new interesting properties, initially containing no
	 * properties and a maximal cost of infinite.
	 */
	public InterestingProperties() {
		// instantiate the maximal costs to the possible maximum
		this.maximalCosts = new Costs(0, 0);

		this.globalProps = new GlobalProperties();
		this.localProps = new LocalProperties();
	}

	/**
	 * Private constructor for cloning purposes.
	 * 
	 * @param maximalCosts
	 *        The maximal costs for this new object.
	 * @param globalProps
	 *        The global properties for this new object.
	 * @param localProps
	 *        The local properties for this new object.
	 */
	private InterestingProperties(Costs maximalCosts, GlobalProperties globalProps, LocalProperties localProps) {
		this.maximalCosts = maximalCosts;
		this.globalProps = globalProps;
		this.localProps = localProps;
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the maximal costs that that it may take to establish these interesting properties, before
	 * they become worthless.
	 * 
	 * @return The maximal costs to establish these properties.
	 */
	public Costs getMaximalCosts() {
		return maximalCosts;
	}

	/**
	 * Gets the interesting local properties.
	 * 
	 * @return The interesting local properties.
	 */
	public LocalProperties getLocalProperties() {
		return localProps;
	}

	/**
	 * Gets the interesting global properties.
	 * 
	 * @return The interesting global properties.
	 */
	public GlobalProperties getGlobalProperties() {
		return globalProps;
	}

	/**
	 * Checks, if the given <tt>InterestingProperties</tt> object has the same properties as this one.
	 * This method is a lesser version of the <code>equals(...)</code> method, as it does not take the
	 * costs into account.
	 * 
	 * @param other
	 *        The object to test for equal properties.
	 * @return True, if the given object has equal properties, false otherwise.
	 */
	public boolean hasEqualProperties(InterestingProperties other) {
		return globalProps.equals(other.globalProps) && localProps.equals(other.localProps);
	}

	/**
	 * Checks, if the interesting properties are met by the given node. The interesting properties are met,
	 * if the global and local properties are met.
	 * 
	 * @param node
	 *        The node to test.
	 * @return True, if the node meets the properties, false otherwise.
	 */
	public boolean isMetBy(PlanNode node) {
		return globalProps.isMetBy(node.getGlobalProperties()) && localProps.isMetBy(node.getLocalProperties());
	}
	
	
	
	public InterestingProperties filterByCodeAnnotations(OptimizerNode node, int input)
	{
		final GlobalProperties gp = this.globalProps.filterByNodesConstantSet(node, input);
		final LocalProperties lp = this.localProps.filterByNodesConstantSet(node, input);
		
		if (gp != this.globalProps || lp != this.localProps) {
			if ((gp == null || gp.isTrivial()) && (lp == null || lp.isTrivial())) {
				return null;
			} else {
				return new InterestingProperties(this.maximalCosts,
						gp == null ? new GlobalProperties() : gp, lp == null ? new LocalProperties() : lp);
			}
		} else {
			return this;
		}
	}
	
	public Channel createChannelRealizingProperties(PlanNode inputNode) {
		final Channel c = new Channel(inputNode);
		
		if (this.globalProps.isMetBy(inputNode.getGlobalProperties())) {
			c.setShipStrategy(ShipStrategyType.FORWARD);
		} else {
			this.globalProps.parameterizeChannel(c);
		}
		
		final LocalProperties lps = c.getLocalPropertiesAfterShippingOnly();
		if (this.localProps.isMetBy(lps)) {
			c.setLocalStrategy(LocalStrategy.NONE);
		} else {
			this.localProps.parameterizeChannel(c);
		}
		return c;
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
		result = prime * result + ((maximalCosts == null) ? 0 : maximalCosts.hashCode());
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
					this.maximalCosts.equals(other.maximalCosts);
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
		return "InterestingProperties [maximalCosts=" + maximalCosts + ", globalProps=" + globalProps + ", localProps="
			+ localProps + "]";
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public InterestingProperties clone() {
		return new InterestingProperties(this.maximalCosts.clone(), 
			this.globalProps.clone(), this.localProps.clone());
	}

	// ------------------------------------------------------------------------

	/**
	 * Adds the given interesting properties to another collection, if no other interesting properties object
	 * subsumes that one. An interesting property object is subsumed by another one, if it has the same properties, but
	 * a higher maximal cost. If one of the given candidates to be added subsumed one of the contained interesting
	 * property objects, it replaces the contained one.
	 * NOTE: This method adds copies of the interesting properties that are to be added. None of the original
	 * objects will be added. It does however modify the costs of the contained objects, if one of the
	 * interesting properties to add has a higher maximal cost limit.
	 * 
	 * @param properties
	 *        The collection of interesting properties to which the other collection is added.
	 * @param toMerge
	 *        The properties that is added / merged into the previous collection.
	 */
	public static void mergeUnionOfInterestingProperties(
			List<InterestingProperties> properties, InterestingProperties toMerge)
	{
		// go through all existing property sets
		for (int i = 0; i < properties.size(); i++) {
			InterestingProperties toCheck = properties.get(i);
			if (toCheck.hasEqualProperties(toMerge)) {
				// the properties are equal. keep the one with the higher maximal cost,
				// because it indicates, that the properties are worth more.
				if (toMerge.getMaximalCosts().compareTo(toCheck.getMaximalCosts()) > 0) {
					properties.set(i, toMerge);
				}
				return;
			}
		}
		// if it was not subsumed, add it
		properties.add(toMerge);
	}

	/**
	 * Adds all of the given interesting properties to another collection, if no other interesting properties object
	 * subsumes that one. An interesting property object is subsumed by another one, if it has the same properties, but
	 * a higher maximal cost. If one of the given candidates to be added subsumed one of the contained interesting
	 * property objects, it replaces the contained one.
	 * NOTE: This method adds copies of the interesting properties that are to be added. None of the original
	 * objects will be added. It does however modify the costs of the contained objects, if one of the
	 * interesting properties to add has a higher maximal cost limit.
	 * 
	 * @param properties
	 *        The collection of interesting properties to which the other collection is added.
	 * @param toMerge
	 *        The set of properties that is added / merged into the previous collection.
	 */
	public static void mergeUnionOfInterestingProperties(List<InterestingProperties> properties,
			List<InterestingProperties> toMerge) {
		// go through all properties that are candidates to be added
		for (InterestingProperties candidate : toMerge) {
			mergeUnionOfInterestingProperties(properties, candidate);
		}
	}
	
	
	/**
	 * @param props
	 * @param node
	 * @param input
	 * @return
	 */
	public static final List<InterestingProperties> filterInterestingPropertiesForInput(
		List<InterestingProperties> props, OptimizerNode node, int input)
	{
		List<InterestingProperties> preserved = null;
		
		for (InterestingProperties p : props) {
			final InterestingProperties filteredProps = p.filterByCodeAnnotations(node, input);
			if (filteredProps == null) {
				continue;
			}
			
			final GlobalProperties topDownAdjustedGP = filteredProps.globalProps.createInterestingGlobalPropertiesTopDownSubset(node, input);
			if (topDownAdjustedGP == null && filteredProps.localProps.isTrivial()) 
				continue;
			
			if (preserved == null) {
				preserved = new ArrayList<InterestingProperties>();
			}
			
			final InterestingProperties toAdd = topDownAdjustedGP == filteredProps.getGlobalProperties() ? filteredProps :
				new InterestingProperties(filteredProps.getMaximalCosts(), topDownAdjustedGP, filteredProps.localProps);
			mergeUnionOfInterestingProperties(preserved, toAdd);
		}
		return preserved == null ? new ArrayList<InterestingProperties>() : preserved;
	}
}
