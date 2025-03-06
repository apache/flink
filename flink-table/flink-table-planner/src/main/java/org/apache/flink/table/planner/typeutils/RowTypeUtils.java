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

package org.apache.flink.table.planner.typeutils;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Utils for deriving row types of {@link org.apache.calcite.rel.RelNode}s. */
public class RowTypeUtils {

    public static String getUniqueName(String oldName, List<String> checklist) {
        return getUniqueName(Collections.singletonList(oldName), checklist).get(0);
    }

    public static List<String> getUniqueName(List<String> oldNames, List<String> checklist) {
        List<String> result = new ArrayList<>();
        for (String oldName : oldNames) {
            if (checklist.contains(oldName) || result.contains(oldName)) {
                int suffix = -1;
                String changedName;
                do {
                    suffix++;
                    changedName = oldName + "_" + suffix;
                } while (checklist.contains(changedName) || result.contains(changedName));
                result.add(changedName);
            } else {
                result.add(oldName);
            }
        }
        return result;
    }

    /**
     * Returns projected {@link RowType} by given projection indexes over original {@link RowType}.
     * Will raise an error when projection index beyond the field count of original rowType.
     *
     * @param rowType source row type
     * @param projection indexes array
     * @return projected {@link RowType}
     */
    public static RowType projectRowType(@Nonnull RowType rowType, @Nonnull int[] projection)
            throws IllegalArgumentException {
        final int fieldCnt = rowType.getFieldCount();
        return RowType.of(
                Arrays.stream(projection)
                        .mapToObj(
                                index -> {
                                    if (index >= fieldCnt) {
                                        throw new IllegalArgumentException(
                                                String.format(
                                                        "Invalid projection index: %d of source rowType size: %d",
                                                        index, fieldCnt));
                                    }
                                    return rowType.getTypeAt(index);
                                })
                        .toArray(LogicalType[]::new),
                Arrays.stream(projection)
                        .mapToObj(index -> rowType.getFieldNames().get(index))
                        .toArray(String[]::new));
    }
}
