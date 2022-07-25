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

package org.apache.flink.table.runtime.operators.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.python.embedded.AbstractEmbeddedPythonFunctionOperator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.python.utils.StreamRecordRowDataWrappingCollector;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Base class for all stream operators to execute Python Stateless Functions in embedded Python
 * environment.
 */
@Internal
public abstract class AbstractEmbeddedStatelessFunctionOperator
        extends AbstractEmbeddedPythonFunctionOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    /** The offsets of user-defined function inputs. */
    protected final int[] udfInputOffsets;

    /** The input logical type. */
    protected final RowType inputType;

    /** The user-defined function input logical type. */
    protected final RowType udfInputType;

    /** The user-defined function output logical type. */
    protected final RowType udfOutputType;

    /** The GenericRowData reused holding the execution result of python udf. */
    protected transient GenericRowData reuseResultRowData;

    /** The collector used to collect records. */
    protected transient StreamRecordRowDataWrappingCollector rowDataWrapper;

    protected transient PythonTypeUtils.DataConverter[] userDefinedFunctionInputConverters;
    protected transient Object[] userDefinedFunctionInputArgs;
    protected transient PythonTypeUtils.DataConverter[] userDefinedFunctionOutputConverters;

    public AbstractEmbeddedStatelessFunctionOperator(
            Configuration config,
            RowType inputType,
            RowType udfInputType,
            RowType udfOutputType,
            int[] udfInputOffsets) {
        super(config);
        this.inputType = Preconditions.checkNotNull(inputType);
        this.udfInputType = Preconditions.checkNotNull(udfInputType);
        this.udfOutputType = Preconditions.checkNotNull(udfOutputType);
        this.udfInputOffsets = Preconditions.checkNotNull(udfInputOffsets);
    }

    @Override
    public void open() throws Exception {
        super.open();
        rowDataWrapper = new StreamRecordRowDataWrappingCollector(output);
        reuseResultRowData = new GenericRowData(udfOutputType.getFieldCount());
        RowType userDefinedFunctionInputType =
                new RowType(
                        Arrays.stream(udfInputOffsets)
                                .mapToObj(i -> inputType.getFields().get(i))
                                .collect(Collectors.toList()));
        userDefinedFunctionInputConverters =
                userDefinedFunctionInputType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(PythonTypeUtils::toDataConverter)
                        .toArray(PythonTypeUtils.DataConverter[]::new);
        userDefinedFunctionInputArgs = new Object[udfInputOffsets.length];
        userDefinedFunctionOutputConverters =
                udfOutputType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(PythonTypeUtils::toDataConverter)
                        .toArray(PythonTypeUtils.DataConverter[]::new);
    }
}
