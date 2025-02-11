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

package org.apache.flink.table.runtime.operators.process;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/**
 * Serializable representation of {@link TableSemantics} that will serve {@link
 * ProcessTableFunction.Context#tableSemanticsFor(String)} during runtime.
 */
public class RuntimeTableSemantics implements TableSemantics, Serializable {

    private static final long serialVersionUID = 1L;

    private final String argName;
    private final int inputIndex;
    private final DataType dataType;
    private final int[] partitionByColumns;
    private final byte[] expectedChanges;
    private final boolean passColumnsThrough;
    private final boolean hasSetSemantics;

    private transient ChangelogMode changelogMode;

    public RuntimeTableSemantics(
            String argName,
            int inputIndex,
            DataType dataType,
            int[] partitionByColumns,
            byte[] expectedChanges,
            boolean passColumnsThrough,
            boolean hasSetSemantics) {
        this.argName = argName;
        this.inputIndex = inputIndex;
        this.dataType = dataType;
        this.partitionByColumns = partitionByColumns;
        this.expectedChanges = expectedChanges;
        this.passColumnsThrough = passColumnsThrough;
        this.hasSetSemantics = hasSetSemantics;
    }

    public String getArgName() {
        return argName;
    }

    public int getInputIndex() {
        return inputIndex;
    }

    public boolean passColumnsThrough() {
        return passColumnsThrough;
    }

    public boolean hasSetSemantics() {
        return hasSetSemantics;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public int[] partitionByColumns() {
        return partitionByColumns;
    }

    @Override
    public int[] orderByColumns() {
        return new int[0];
    }

    @Override
    public int timeColumn() {
        return -1;
    }

    @Override
    public List<String> coPartitionArgs() {
        return List.of();
    }

    @Override
    public Optional<ChangelogMode> changelogMode() {
        if (changelogMode == null) {
            final ChangelogMode.Builder builder = ChangelogMode.newBuilder();
            for (byte expectedChange : expectedChanges) {
                builder.addContainedKind(RowKind.fromByteValue(expectedChange));
            }
            changelogMode = builder.build();
        }
        return Optional.of(changelogMode);
    }
}
