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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecLegacySink;
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Stream {@link ExecNode} to to write data into an external sink defined by a {@link TableSink}.
 *
 * @param <T> The return type of the {@link TableSink}.
 */
public class StreamExecLegacySink<T> extends CommonExecLegacySink<T> implements StreamExecNode<T> {

    public StreamExecLegacySink(
            TableSink<T> tableSink,
            @Nullable String[] upsertKeys,
            boolean needRetraction,
            InputProperty inputProperty,
            LogicalType outputType,
            String description) {
        super(
                tableSink,
                upsertKeys,
                needRetraction,
                true, // isStreaming
                inputProperty,
                outputType,
                description);
    }

    protected RowType checkAndConvertInputTypeIfNeeded(RowType inputRowType) {
        final List<Integer> rowtimeFieldIndices = new ArrayList<>();
        for (int i = 0; i < inputRowType.getFieldCount(); ++i) {
            if (TypeCheckUtils.isRowTime(inputRowType.getTypeAt(i))) {
                rowtimeFieldIndices.add(i);
            }
        }

        if (rowtimeFieldIndices.size() > 1) {
            throw new TableException(
                    String.format(
                            "Found more than one rowtime field: [%s] in "
                                    + "the table that should be converted to a DataStream.\n"
                                    + "Please select the rowtime field that should be used as event-time timestamp for the "
                                    + "DataStream by casting all other fields to TIMESTAMP.",
                            rowtimeFieldIndices.stream()
                                    .map(i -> inputRowType.getFieldNames().get(i))
                                    .collect(Collectors.joining(", "))));
        } else if (rowtimeFieldIndices.size() == 1) {
            LogicalType[] convertedFieldTypes =
                    inputRowType.getChildren().stream()
                            .map(
                                    t -> {
                                        if (TypeCheckUtils.isRowTime(t)) {
                                            if (TypeCheckUtils.isTimestampWithLocalZone(t)) {
                                                return new LocalZonedTimestampType(3);
                                            } else {
                                                return new TimestampType(3);
                                            }
                                        } else {
                                            return t;
                                        }
                                    })
                            .toArray(LogicalType[]::new);

            return RowType.of(
                    convertedFieldTypes, inputRowType.getFieldNames().toArray(new String[0]));
        } else {
            return inputRowType;
        }
    }
}
