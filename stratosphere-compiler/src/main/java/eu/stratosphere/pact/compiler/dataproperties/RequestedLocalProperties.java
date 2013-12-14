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

import java.util.Arrays;

import eu.stratosphere.api.operators.Ordering;
import eu.stratosphere.api.operators.util.FieldList;
import eu.stratosphere.api.operators.util.FieldSet;
import eu.stratosphere.pact.compiler.plan.OptimizerNode;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.util.Utils;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;

/**
 * This class represents local properties of the data. A local property is a property that exists
 * within the data of a single partition.
 */
public class RequestedLocalProperties implements Cloneable {

	public static final RequestedLocalProperties DEFAULT_PROPERTIES = null;
	
	// --------------------------------------------------------------------------------------------
	
	Ordering ordering;			// order inside a partition, null if not ordered

	FieldSet groupedFields;		// fields by which the stream is grouped. null if not grouped.

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Default constructor for trivial local properties. No order, no grouping, no uniqueness.
	 */
	public RequestedLocalProperties() {}
	
	/**
	 * Creates interesting properties for the given ordering.
	 * 
	 * @param ordering The interesting ordering.
	 */
	public RequestedLocalProperties(Ordering ordering) {
		this.ordering = ordering;
	}
	
	/**
	 * Creates interesting properties for the given grouping.
	 * 
	 * @param groupedFields The set of fields whose grouping is interesting.
	 */
	public RequestedLocalProperties(FieldSet groupedFields) {
		this.groupedFields = groupedFields;
	}
	
	/**
	 * This constructor is used only for internal copy creation.
	 * 
	 * @param ordering The ordering represented by these local properties.
	 * @param groupedFields The grouped fields for these local properties.
	 */
	private RequestedLocalProperties(Ordering ordering, FieldSet groupedFields) {
		this.ordering = ordering;
		this.groupedFields = groupedFields;
	}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the key order.
	 * 
	 * @return The key order, or <code>null</code> if nothing is ordered.
	 */
	public Ordering getOrdering() {
		return ordering;
	}
	
	/**
	 * Sets the order for these interesting local properties.
	 * 
	 * @param ordering The order to set.
	 */
	public void setOrdering(Ordering ordering) {
		this.ordering = ordering;
	}
	
	/**
	 * Gets the grouped fields.
	 * 
	 * @return The grouped fields, or <code>null</code> if nothing is grouped.
	 */
	public FieldSet getGroupedFields() {
		return this.groupedFields;
	}
	
	/**
	 * Sets the fields that are grouped in these data properties.
	 * 
	 * @param groupedFields The fields that are grouped in these data properties.
	 */
	public void setGroupedFields(FieldSet groupedFields) {
		this.groupedFields = groupedFields;	
	}
	
	/**
	 * Checks, if the properties in this object are trivial, i.e. only standard values.
	 */
	public boolean isTrivial() {
		return ordering == null && this.groupedFields == null;
	}

	/**
	 * This method resets the local properties to a state where no properties are given.
	 */
	public void reset() {
		this.ordering = null;
		this.groupedFields = null;
	}
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Filters these properties by what can be preserved through a user function's constant fields set.
	 * Since interesting properties are filtered top-down, anything that partially destroys the ordering
	 * makes the properties uninteresting.
	 * 
	 * @param node The optimizer node that potentially modifies the properties.
	 * @param input The input of the node which is relevant.
	 * 
	 * @return True, if the resulting properties are non trivial.
	 */
	public RequestedLocalProperties filterByNodesConstantSet(OptimizerNode node, int input) {
		if (this.ordering != null) {
			final FieldList involvedIndexes = this.ordering.getInvolvedIndexes();
			for (int i = 0; i < involvedIndexes.size(); i++) {
				if (!node.isFieldConstant(input, involvedIndexes.get(i))) {
					return null;
				}
			}
		} else if (this.groupedFields != null) {
			// check, whether the local key grouping is preserved
			for (Integer index : this.groupedFields) {
				if (!node.isFieldConstant(input, index)) {
					return null;
				}
			}
		}
		return this;
	}

	/**
	 * Checks, if this set of properties, as interesting properties, is met by the given
	 * properties.
	 * 
	 * @param other
	 *        The properties for which to check whether they meet these properties.
	 * @return True, if the properties are met, false otherwise.
	 */
	public boolean isMetBy(LocalProperties other) {
		if (this.ordering != null) {
			// we demand an ordering
			return other.getOrdering() != null && this.ordering.isMetBy(other.getOrdering());
		} else if (this.groupedFields != null) {
			// check if the other fields are unique
			if (other.getGroupedFields() != null && other.getGroupedFields().isValidUnorderedPrefix(this.groupedFields)) {
				return true;
			} else  {
				return other.areFieldsUnique(this.groupedFields);
			}
		} else {
			return true;
		}
	}
	
	/**
	 * Parameterizes the local strategy fields of a channel such that the channel produces the desired local properties.
	 * 
	 * @param channel The channel to parameterize.
	 */
	public void parameterizeChannel(Channel channel) {
		if (this.ordering != null) {
			channel.setLocalStrategy(LocalStrategy.SORT, this.ordering.getInvolvedIndexes(), this.ordering.getFieldSortDirections());
		} else if (this.groupedFields != null) {
			boolean[] dirs = new boolean[this.groupedFields.size()];
			Arrays.fill(dirs, true);
			channel.setLocalStrategy(LocalStrategy.SORT, Utils.createOrderedFromSet(this.groupedFields), dirs);
		}
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.ordering == null ? 0 : this.ordering.hashCode());
		result = prime * result + (this.groupedFields == null ? 0 : this.groupedFields.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof RequestedLocalProperties) {
			final RequestedLocalProperties other = (RequestedLocalProperties) obj;
			return (this.ordering == other.ordering || (this.ordering != null && this.ordering.equals(other.ordering))) &&
				(this.groupedFields == other.groupedFields || (this.groupedFields != null && this.groupedFields.equals(other.groupedFields)));
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return "Requested Local Properties [ordering=" + this.ordering + ", grouped=" + this.groupedFields + "]";
	}

	@Override
	public RequestedLocalProperties clone() {
		return new RequestedLocalProperties(this.ordering, this.groupedFields);
	}
}
