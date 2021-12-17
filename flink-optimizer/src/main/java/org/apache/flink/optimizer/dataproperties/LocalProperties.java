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
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.common.operators.util.FieldSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * This class represents local properties of the data. A local property is a property that exists
 * within the data of a single partition, such as sort order, or data grouping.
 */
public class LocalProperties implements Cloneable {

    public static final Logger LOG = LoggerFactory.getLogger(GlobalProperties.class);

    public static final LocalProperties EMPTY = new LocalProperties();

    // --------------------------------------------------------------------------------------------

    private Ordering ordering; // order inside a partition, null if not ordered

    private FieldList groupedFields; // fields by which the stream is grouped. null if not grouped.

    private Set<FieldSet> uniqueFields; // fields whose value combination is unique in the stream

    // --------------------------------------------------------------------------------------------

    /** Default constructor for trivial local properties. No order, no grouping, no uniqueness. */
    public LocalProperties() {}

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
     * Gets the grouped fields.
     *
     * @return The grouped fields, or <code>null</code> if nothing is grouped.
     */
    public FieldList getGroupedFields() {
        return this.groupedFields;
    }

    /**
     * Gets the fields whose combination is unique within the data set.
     *
     * @return The unique field combination, or <code>null</code> if nothing is unique.
     */
    public Set<FieldSet> getUniqueFields() {
        return this.uniqueFields;
    }

    /**
     * Checks whether the given set of fields is unique, as specified in these local properties.
     *
     * @param set The set to check.
     * @return True, if the given column combination is unique, false if not.
     */
    public boolean areFieldsUnique(FieldSet set) {
        return this.uniqueFields != null && this.uniqueFields.contains(set);
    }

    /**
     * Adds a combination of fields that are unique in these data properties.
     *
     * @param uniqueFields The fields that are unique in these data properties.
     */
    public LocalProperties addUniqueFields(FieldSet uniqueFields) {
        LocalProperties copy = clone();

        if (copy.uniqueFields == null) {
            copy.uniqueFields = new HashSet<FieldSet>();
        }
        copy.uniqueFields.add(uniqueFields);
        return copy;
    }

    public LocalProperties clearUniqueFieldSets() {
        if (this.uniqueFields == null || this.uniqueFields.isEmpty()) {
            return this;
        } else {
            LocalProperties copy = new LocalProperties();
            copy.ordering = this.ordering;
            copy.groupedFields = this.groupedFields;
            return copy;
        }
    }

    /** Checks, if the properties in this object are trivial, i.e. only standard values. */
    public boolean isTrivial() {
        return ordering == null && this.groupedFields == null && this.uniqueFields == null;
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Filters these LocalProperties by the fields that are forwarded to the output as described by
     * the SemanticProperties.
     *
     * @param props The semantic properties holding information about forwarded fields.
     * @param input The index of the input.
     * @return The filtered LocalProperties
     */
    public LocalProperties filterBySemanticProperties(SemanticProperties props, int input) {

        if (props == null) {
            throw new NullPointerException("SemanticProperties may not be null.");
        }

        LocalProperties returnProps = new LocalProperties();

        // check if sorting is preserved
        if (this.ordering != null) {
            Ordering newOrdering = new Ordering();

            for (int i = 0; i < this.ordering.getInvolvedIndexes().size(); i++) {
                int sourceField = this.ordering.getInvolvedIndexes().get(i);
                FieldSet targetField = props.getForwardingTargetFields(input, sourceField);
                if (targetField == null || targetField.size() == 0) {
                    if (i == 0) {
                        // order fully destroyed
                        newOrdering = null;
                        break;
                    } else {
                        // order partially preserved
                        break;
                    }
                } else {
                    // use any field of target fields for now.  We should use something like field
                    // equivalence sets in the future.
                    if (targetField.size() > 1) {
                        LOG.warn(
                                "Found that a field is forwarded to more than one target field in "
                                        + "semantic forwarded field information. Will only use the field with the lowest index.");
                    }
                    newOrdering.appendOrdering(
                            targetField.toArray()[0],
                            this.ordering.getType(i),
                            this.ordering.getOrder(i));
                }
            }

            returnProps.ordering = newOrdering;
            if (newOrdering != null) {
                returnProps.groupedFields = newOrdering.getInvolvedIndexes();
            } else {
                returnProps.groupedFields = null;
            }
        }
        // check if grouping is preserved
        else if (this.groupedFields != null) {
            FieldList newGroupedFields = new FieldList();

            for (Integer sourceField : this.groupedFields) {
                FieldSet targetField = props.getForwardingTargetFields(input, sourceField);
                if (targetField == null || targetField.size() == 0) {
                    newGroupedFields = null;
                    break;
                } else {
                    // use any field of target fields for now.  We should use something like field
                    // equivalence sets in the future.
                    if (targetField.size() > 1) {
                        LOG.warn(
                                "Found that a field is forwarded to more than one target field in "
                                        + "semantic forwarded field information. Will only use the field with the lowest index.");
                    }
                    newGroupedFields = newGroupedFields.addField(targetField.toArray()[0]);
                }
            }
            returnProps.groupedFields = newGroupedFields;
        }

        if (this.uniqueFields != null) {
            Set<FieldSet> newUniqueFields = new HashSet<FieldSet>();
            for (FieldSet fields : this.uniqueFields) {
                FieldSet newFields = new FieldSet();
                for (Integer sourceField : fields) {
                    FieldSet targetField = props.getForwardingTargetFields(input, sourceField);

                    if (targetField == null || targetField.size() == 0) {
                        newFields = null;
                        break;
                    } else {
                        // use any field of target fields for now.  We should use something like
                        // field equivalence sets in the future.
                        if (targetField.size() > 1) {
                            LOG.warn(
                                    "Found that a field is forwarded to more than one target field in "
                                            + "semantic forwarded field information. Will only use the field with the lowest index.");
                        }
                        newFields = newFields.addField(targetField.toArray()[0]);
                    }
                }
                if (newFields != null) {
                    newUniqueFields.add(newFields);
                }
            }

            if (!newUniqueFields.isEmpty()) {
                returnProps.uniqueFields = newUniqueFields;
            } else {
                returnProps.uniqueFields = null;
            }
        }

        return returnProps;
    }
    // --------------------------------------------------------------------------------------------

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.ordering == null ? 0 : this.ordering.hashCode());
        result = prime * result + (this.groupedFields == null ? 0 : this.groupedFields.hashCode());
        result = prime * result + (this.uniqueFields == null ? 0 : this.uniqueFields.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LocalProperties) {
            final LocalProperties other = (LocalProperties) obj;
            return (ordering == other.getOrdering()
                            || (ordering != null && ordering.equals(other.getOrdering())))
                    && (groupedFields == other.getGroupedFields()
                            || (groupedFields != null
                                    && groupedFields.equals(other.getGroupedFields())))
                    && (uniqueFields == other.getUniqueFields()
                            || (uniqueFields != null
                                    && uniqueFields.equals(other.getUniqueFields())));
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "LocalProperties [ordering="
                + this.ordering
                + ", grouped="
                + this.groupedFields
                + ", unique="
                + this.uniqueFields
                + "]";
    }

    @Override
    public LocalProperties clone() {
        LocalProperties copy = new LocalProperties();
        copy.ordering = this.ordering;
        copy.groupedFields = this.groupedFields;
        copy.uniqueFields =
                (this.uniqueFields == null ? null : new HashSet<FieldSet>(this.uniqueFields));
        return copy;
    }

    // --------------------------------------------------------------------------------------------

    public static LocalProperties combine(LocalProperties lp1, LocalProperties lp2) {
        if (lp1.ordering != null) {
            return lp1;
        } else if (lp2.ordering != null) {
            return lp2;
        } else if (lp1.groupedFields != null) {
            return lp1;
        } else if (lp2.groupedFields != null) {
            return lp2;
        } else if (lp1.uniqueFields != null && !lp1.uniqueFields.isEmpty()) {
            return lp1;
        } else if (lp2.uniqueFields != null && !lp2.uniqueFields.isEmpty()) {
            return lp2;
        } else {
            return lp1;
        }
    }

    // --------------------------------------------------------------------------------------------

    public static LocalProperties forOrdering(Ordering o) {
        LocalProperties props = new LocalProperties();
        props.ordering = o;
        props.groupedFields = o.getInvolvedIndexes();
        return props;
    }

    public static LocalProperties forGrouping(FieldList groupedFields) {
        LocalProperties props = new LocalProperties();
        props.groupedFields = groupedFields;
        return props;
    }
}
