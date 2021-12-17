/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.optimizer.dataproperties;

import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.util.Utils;
import org.apache.flink.runtime.operators.util.LocalStrategy;

import java.util.Arrays;

/**
 * This class represents the local properties of the data that are requested by an operator. Local
 * properties are the properties within one partition. Operators request the local properties they
 * need for correct execution. Here are some example local properties requested by certain
 * operators:
 *
 * <ul>
 *   <li>"groupBy/reduce" will request the data to be grouped on the key fields.
 *   <li>A sort-merge join will request the data from each input to be sorted on the respective join
 *       key.
 * </ul>
 */
public class RequestedLocalProperties implements Cloneable {

    private Ordering ordering; // order inside a partition, null if not ordered

    private FieldSet groupedFields; // fields by which the stream is grouped. null if not grouped.

    // --------------------------------------------------------------------------------------------

    /** Default constructor for trivial local properties. No order, no grouping, no uniqueness. */
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

    /** Checks, if the properties in this object are trivial, i.e. only standard values. */
    public boolean isTrivial() {
        return ordering == null && this.groupedFields == null;
    }

    /** This method resets the local properties to a state where no properties are given. */
    public void reset() {
        this.ordering = null;
        this.groupedFields = null;
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Filters these properties by what can be preserved by the given SemanticProperties when
     * propagated down to the given input.
     *
     * @param props The SemanticProperties which define which fields are preserved.
     * @param input The index of the operator's input.
     * @return The filtered RequestedLocalProperties
     */
    public RequestedLocalProperties filterBySemanticProperties(
            SemanticProperties props, int input) {

        // no semantic properties, all local properties are filtered
        if (props == null) {
            throw new NullPointerException("SemanticProperties may not be null.");
        }

        if (this.ordering != null) {
            Ordering newOrdering = new Ordering();

            for (int i = 0; i < this.ordering.getInvolvedIndexes().size(); i++) {
                int targetField = this.ordering.getInvolvedIndexes().get(i);
                int sourceField = props.getForwardingSourceField(input, targetField);
                if (sourceField >= 0) {
                    newOrdering.appendOrdering(
                            sourceField, this.ordering.getType(i), this.ordering.getOrder(i));
                } else {
                    return null;
                }
            }
            return new RequestedLocalProperties(newOrdering);

        } else if (this.groupedFields != null) {
            FieldSet newGrouping = new FieldSet();

            // check, whether the local key grouping is preserved
            for (Integer targetField : this.groupedFields) {
                int sourceField = props.getForwardingSourceField(input, targetField);
                if (sourceField >= 0) {
                    newGrouping = newGrouping.addField(sourceField);
                } else {
                    return null;
                }
            }
            return new RequestedLocalProperties(newGrouping);
        } else {
            return null;
        }
    }

    /**
     * Checks, if this set of properties, as interesting properties, is met by the given properties.
     *
     * @param other The properties for which to check whether they meet these properties.
     * @return True, if the properties are met, false otherwise.
     */
    public boolean isMetBy(LocalProperties other) {
        if (this.ordering != null) {
            // we demand an ordering
            return other.getOrdering() != null && this.ordering.isMetBy(other.getOrdering());
        } else if (this.groupedFields != null) {
            // check if the other fields are unique
            if (other.getGroupedFields() != null
                    && other.getGroupedFields().isValidUnorderedPrefix(this.groupedFields)) {
                return true;
            } else {
                return other.areFieldsUnique(this.groupedFields);
            }
        } else {
            return true;
        }
    }

    /**
     * Parametrizes the local strategy fields of a channel such that the channel produces the
     * desired local properties.
     *
     * @param channel The channel to parametrize.
     */
    public void parameterizeChannel(Channel channel) {
        LocalProperties current = channel.getLocalProperties();

        if (isMetBy(current)) {
            // we are met, all is good
            channel.setLocalStrategy(LocalStrategy.NONE);
        } else if (this.ordering != null) {
            channel.setLocalStrategy(
                    LocalStrategy.SORT,
                    this.ordering.getInvolvedIndexes(),
                    this.ordering.getFieldSortDirections());
        } else if (this.groupedFields != null) {
            boolean[] dirs = new boolean[this.groupedFields.size()];
            Arrays.fill(dirs, true);
            channel.setLocalStrategy(
                    LocalStrategy.SORT, Utils.createOrderedFromSet(this.groupedFields), dirs);
        } else {
            channel.setLocalStrategy(LocalStrategy.NONE);
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
            return (this.ordering == other.ordering
                            || (this.ordering != null && this.ordering.equals(other.ordering)))
                    && (this.groupedFields == other.groupedFields
                            || (this.groupedFields != null
                                    && this.groupedFields.equals(other.groupedFields)));
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "Requested Local Properties [ordering="
                + this.ordering
                + ", grouped="
                + this.groupedFields
                + "]";
    }

    @Override
    public RequestedLocalProperties clone() {
        return new RequestedLocalProperties(this.ordering, this.groupedFields);
    }
}
