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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.SortMergeJoinFunction;
import org.apache.flink.table.runtime.operators.join.SortMergeJoinOperator;
import org.apache.flink.table.types.logical.RowType;

import java.util.stream.IntStream;

/** Utility for {@link SortMergeJoinOperator}. */
public class SorMergeJoinOperatorUtil {

    public static SortMergeJoinFunction getSortMergeJoinFunction(
            ClassLoader classLoader,
            ExecNodeConfig config,
            FlinkJoinType joinType,
            RowType leftType,
            RowType rightType,
            int[] leftKeys,
            int[] rightKeys,
            RowType keyType,
            boolean leftIsSmaller,
            boolean[] filterNulls,
            GeneratedJoinCondition condFunc,
            double externalBufferMemRatio) {
        int[] keyPositions = IntStream.range(0, leftKeys.length).toArray();

        int maxNumFileHandles =
                config.get(ExecutionConfigOptions.TABLE_EXEC_SORT_MAX_NUM_FILE_HANDLES);
        boolean compressionEnabled =
                config.get(ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED);
        int compressionBlockSize =
                (int)
                        config.get(ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE)
                                .getBytes();
        boolean asyncMergeEnabled =
                config.get(ExecutionConfigOptions.TABLE_EXEC_SORT_ASYNC_MERGE_ENABLED);

        SortCodeGenerator leftSortGen =
                SortUtil.newSortGen(config, classLoader, leftKeys, leftType);
        SortCodeGenerator rightSortGen =
                SortUtil.newSortGen(config, classLoader, rightKeys, rightType);

        return new SortMergeJoinFunction(
                externalBufferMemRatio,
                joinType,
                leftIsSmaller,
                maxNumFileHandles,
                compressionEnabled,
                compressionBlockSize,
                asyncMergeEnabled,
                condFunc,
                ProjectionCodeGenerator.generateProjection(
                        new CodeGeneratorContext(config, classLoader),
                        "SMJProjection",
                        leftType,
                        keyType,
                        leftKeys),
                ProjectionCodeGenerator.generateProjection(
                        new CodeGeneratorContext(config, classLoader),
                        "SMJProjection",
                        rightType,
                        keyType,
                        rightKeys),
                leftSortGen.generateNormalizedKeyComputer("LeftComputer"),
                leftSortGen.generateRecordComparator("LeftComparator"),
                rightSortGen.generateNormalizedKeyComputer("RightComputer"),
                rightSortGen.generateRecordComparator("RightComparator"),
                SortUtil.newSortGen(config, classLoader, keyPositions, keyType)
                        .generateRecordComparator("KeyComparator"),
                filterNulls);
    }

    private SorMergeJoinOperatorUtil() {}
}
