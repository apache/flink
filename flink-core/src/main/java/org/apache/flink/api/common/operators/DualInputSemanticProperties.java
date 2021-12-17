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

package org.apache.flink.api.common.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.util.FieldSet;

import java.util.HashMap;
import java.util.Map;

/** Container for the semantic properties associated to a dual input operator. */
@Internal
public class DualInputSemanticProperties implements SemanticProperties {
    private static final long serialVersionUID = 1L;

    /**
     * Mapping from fields in the source record(s) in the first input to fields in the destination
     * record(s).
     */
    private Map<Integer, FieldSet> fieldMapping1;

    /**
     * Mapping from fields in the source record(s) in the second input to fields in the destination
     * record(s).
     */
    private Map<Integer, FieldSet> fieldMapping2;

    /** Set of fields that are read in the source record(s) from the first input. */
    private FieldSet readFields1;

    /** Set of fields that are read in the source record(s) from the second input. */
    private FieldSet readFields2;

    public DualInputSemanticProperties() {
        this.fieldMapping1 = new HashMap<Integer, FieldSet>();
        this.fieldMapping2 = new HashMap<Integer, FieldSet>();
        this.readFields1 = null;
        this.readFields2 = null;
    }

    @Override
    public FieldSet getForwardingTargetFields(int input, int sourceField) {

        if (input != 0 && input != 1) {
            throw new IndexOutOfBoundsException();
        } else if (input == 0) {

            return fieldMapping1.containsKey(sourceField)
                    ? fieldMapping1.get(sourceField)
                    : FieldSet.EMPTY_SET;
        } else {
            return fieldMapping2.containsKey(sourceField)
                    ? fieldMapping2.get(sourceField)
                    : FieldSet.EMPTY_SET;
        }
    }

    @Override
    public int getForwardingSourceField(int input, int targetField) {
        Map<Integer, FieldSet> fieldMapping;

        if (input != 0 && input != 1) {
            throw new IndexOutOfBoundsException();
        } else if (input == 0) {
            fieldMapping = fieldMapping1;
        } else {
            fieldMapping = fieldMapping2;
        }

        for (Map.Entry<Integer, FieldSet> e : fieldMapping.entrySet()) {
            if (e.getValue().contains(targetField)) {
                return e.getKey();
            }
        }
        return -1;
    }

    @Override
    public FieldSet getReadFields(int input) {
        if (input != 0 && input != 1) {
            throw new IndexOutOfBoundsException();
        }

        if (input == 0) {
            return readFields1;
        } else {
            return readFields2;
        }
    }

    /**
     * Adds, to the existing information, a field that is forwarded directly from the source
     * record(s) in the first input to the destination record(s).
     *
     * @param input the input of the source field
     * @param sourceField the position in the source record
     * @param targetField the position in the destination record
     */
    public void addForwardedField(int input, int sourceField, int targetField) {

        Map<Integer, FieldSet> fieldMapping;

        if (input != 0 && input != 1) {
            throw new IndexOutOfBoundsException();
        } else if (input == 0) {
            fieldMapping = this.fieldMapping1;
        } else {
            fieldMapping = this.fieldMapping2;
        }

        if (isTargetFieldPresent(targetField, fieldMapping)) {
            throw new InvalidSemanticAnnotationException(
                    "Target field " + targetField + " was added twice to input " + input);
        }

        FieldSet targetFields = fieldMapping.get(sourceField);
        if (targetFields != null) {
            fieldMapping.put(sourceField, targetFields.addField(targetField));
        } else {
            fieldMapping.put(sourceField, new FieldSet(targetField));
        }
    }

    private boolean isTargetFieldPresent(int targetField, Map<Integer, FieldSet> fieldMapping) {

        for (FieldSet targetFields : fieldMapping.values()) {
            if (targetFields.contains(targetField)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Adds, to the existing information, field(s) that are read in the source record(s) from the
     * first input.
     *
     * @param input the input of the read fields
     * @param readFields the position(s) in the source record(s)
     */
    public void addReadFields(int input, FieldSet readFields) {

        if (input != 0 && input != 1) {
            throw new IndexOutOfBoundsException();
        } else if (input == 0) {
            this.readFields1 =
                    (this.readFields1 == null)
                            ? readFields.clone()
                            : this.readFields1.addFields(readFields);
        } else {
            this.readFields2 =
                    (this.readFields2 == null)
                            ? readFields.clone()
                            : this.readFields2.addFields(readFields);
        }
    }

    @Override
    public String toString() {
        return "DISP(" + this.fieldMapping1 + "; " + this.fieldMapping2 + ")";
    }
}
