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

package org.apache.flink.table.planner.plan.nodes.exec.spec;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link PartitionSpec} describes how data is partitioned in Rank. */
public class PartitionSpec {

    public static final String FIELD_NAME_FIELDS = "fields";

    /** PartitionSpec makes all data in one partition. */
    public static final PartitionSpec ALL_IN_ONE = new PartitionSpec(new int[0]);

    /** 0-based index of field used in partitioning. */
    @JsonProperty(FIELD_NAME_FIELDS)
    private final int[] fields;

    @JsonCreator
    public PartitionSpec(@JsonProperty(FIELD_NAME_FIELDS) int[] fields) {
        this.fields = checkNotNull(fields);
    }

    /** Gets field index of all fields in input. */
    @JsonIgnore
    public int[] getFieldIndices() {
        return fields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionSpec that = (PartitionSpec) o;
        return Arrays.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(fields);
    }

    @Override
    public String toString() {
        return "Partition{" + "fields=" + Arrays.toString(fields) + '}';
    }
}
