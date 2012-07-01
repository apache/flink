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

//import java.util.ArrayList;
import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.pact.compiler.Costs;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
//import eu.stratosphere.pact.compiler.OutputContract;

/**
 * The interesting properties that a node in the optimizer plan hands to its predecessors. It has the
 * purpose to tell the preceding nodes, which data properties might have the advantage, because they would
 * let the node fulfill its pact cheaper. More on optimization with interesting properties can be found
 * in the works on the volcano- and cascades optimizer framework.
 */
public class InterestingProperties implements Cloneable {
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
		this.maximalCosts = new Costs(Long.MAX_VALUE, Long.MAX_VALUE);

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
	 * Copies the maximal costs from the given costs object.
	 * 
	 * @param c
	 *        The costs object to copy.
	 */
	public void copyMaximalCosts(Costs c) {
		this.maximalCosts.setNetworkCost(c.getNetworkCost());
		this.maximalCosts.setSecondaryStorageCost(c.getSecondaryStorageCost());
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
	public boolean isMetBy(OptimizerNode node) {
		return globalProps.isMetBy(node.getGlobalProperties()) && localProps.isMetBy(node.getLocalProperties());
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
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		InterestingProperties other = (InterestingProperties) obj;
		if (globalProps == null) {
			if (other.globalProps != null)
				return false;
		} else if (!globalProps.equals(other.globalProps))
			return false;
		if (localProps == null) {
			if (other.localProps != null)
				return false;
		} else if (!localProps.equals(other.localProps))
			return false;
		if (maximalCosts == null) {
			if (other.maximalCosts != null)
				return false;
		} else if (!maximalCosts.equals(other.maximalCosts))
			return false;
		return true;
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
	public InterestingProperties clone()
	{
		return new InterestingProperties(maximalCosts.createCopy(), 
			globalProps.createCopy(), localProps.createCopy());
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
	public static void mergeUnionOfInterestingProperties(List<InterestingProperties> properties,
			InterestingProperties toMerge) {
		boolean subsumed = false;

		// go through all existing property sets
		for (InterestingProperties toCheck : properties) {
			if (toCheck.hasEqualProperties(toMerge)) {
				subsumed = true;
				// the properties are equal. keep the one with the higher maximal cost,
				// because it indicates, that the properties are worth more.
				if (toMerge.getMaximalCosts().compareTo(toCheck.getMaximalCosts()) > 0) {
					toCheck.copyMaximalCosts(toMerge.getMaximalCosts());
				}

				break;
			}
		}

		// if it was not subsumes, add it
		if (!subsumed) {
			properties.add(toMerge.clone());
		}
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

//	/**
//	 * Utility method that checks, how the given interesting properties that a node receives from its
//	 * successors, are relevant to its predecessors. That depends, of course, on the output contract,
//	 * as that determines which properties can be inferred to be preserved by the node. The returned
//	 * set will not contain interesting properties objects that are reduced to trivial properties,
//	 * i.e. where all properties have the default value, such as for example <i>none</i> for the
//	 * partitioning.
//	 * 
//	 * @param props
//	 *        The collection of interesting properties that a node receives from its successors.
//	 * @param contract
//	 *        The output contract.
//	 * @return A collection with the interesting properties that are relevant with respect to the given output
//	 *         contract. Contains the same objects as in the input set, with properties accordingly restricted.
//	 *         Returns always a modifiable collection, even if no properties are preserved.
//	 */
//	public static final List<InterestingProperties> filterByOutputContract(List<InterestingProperties> props,
//			OutputContract contract) {
//		// if the output contract is NONE, it basically destroys all properties,
//		// as they always refer to the key, and the key is potentially switched
//		if (contract == OutputContract.None) {
//			return new ArrayList<InterestingProperties>();
//		} else {
//			List<InterestingProperties> preserved = new ArrayList<InterestingProperties>();
//
//			// process all interesting properties
//			for (InterestingProperties p : props) {
//				boolean nonTrivial = p.getGlobalProperties().filterByOutputContract(contract);
//				nonTrivial |= p.getLocalProperties().filterByOutputContract(contract);
//
//				if (nonTrivial) {
//					preserved.add(p);
//				}
//			}
//
//			return preserved;
//		}
//	}
	
	
	public static final List<InterestingProperties> createInterestingPropertiesForInput(List<InterestingProperties> props,
			OptimizerNode node, int input) {
		List<InterestingProperties> preserved = new ArrayList<InterestingProperties>();
		
		for (InterestingProperties p : props) {
//			GlobalProperties preservedGp = p.getGlobalProperties().createCopy();
//			LocalProperties preservedLp = p.getLocalProperties().createCopy();
//			boolean nonTrivial = preservedGp.filterByNodesConstantSet(node, input);
//			nonTrivial |= preservedLp.filterByNodesConstantSet(node, input);
			
			GlobalProperties preservedGp =
					p.getGlobalProperties().createInterestingGlobalProperties(node, input);
			LocalProperties preservedLp = 
					p.getLocalProperties().createInterestingLocalProperties(node, input);

			if (preservedGp != null || preservedLp != null) {
				try {
					if (preservedGp == null) {
						preservedGp = new GlobalProperties();
					}
					if (preservedLp == null) {
						preservedLp = new LocalProperties();
					}
					InterestingProperties newIp = new InterestingProperties(p.getMaximalCosts().clone(), preservedGp, preservedLp);
					mergeUnionOfInterestingProperties(preserved, newIp);
				} catch (CloneNotSupportedException cnse) {
					// should never happen, but propagate just in case
					throw new RuntimeException(cnse);
				}
			}
		}

		return preserved;
	}
}
