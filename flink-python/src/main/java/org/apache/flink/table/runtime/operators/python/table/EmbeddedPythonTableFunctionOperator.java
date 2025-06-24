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

package org.apache.flink.table.runtime.operators.python.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.util.ProtoUtils;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.python.AbstractEmbeddedStatelessFunctionOperator;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import pemja.core.object.PyIterator;

import static org.apache.flink.python.PythonOptions.PYTHON_METRIC_ENABLED;
import static org.apache.flink.python.PythonOptions.PYTHON_PROFILE_ENABLED;
import static org.apache.flink.python.util.ProtoUtils.createFlattenRowTypeCoderInfoDescriptorProto;

/** The Python {@link TableFunction} operator in embedded Python environment. */
@Internal
public class EmbeddedPythonTableFunctionOperator extends AbstractEmbeddedStatelessFunctionOperator {

    private static final long serialVersionUID = 1L;

    /** The Python {@link TableFunction} to be executed. */
    private final PythonFunctionInfo tableFunction;

    /** The correlate join type. */
    private final FlinkJoinType joinType;

    /** The GenericRowData reused holding the null execution result of python udf. */
    private GenericRowData reuseNullResultRowData;

    /** The JoinedRowData reused holding the execution result. */
    private transient JoinedRowData reuseJoinedRow;

    public EmbeddedPythonTableFunctionOperator(
            Configuration config,
            PythonFunctionInfo tableFunction,
            RowType inputType,
            RowType udfInputType,
            RowType udfOutputType,
            FlinkJoinType joinType,
            int[] udfInputOffsets) {
        super(config, inputType, udfInputType, udfOutputType, udfInputOffsets);
        this.tableFunction = Preconditions.checkNotNull(tableFunction);
        Preconditions.checkArgument(
                joinType == FlinkJoinType.INNER || joinType == FlinkJoinType.LEFT,
                "The join type should be inner join or left join");
        this.joinType = joinType;
    }

    @Override
    public void open() throws Exception {
        super.open();
        reuseJoinedRow = new JoinedRowData();
        reuseNullResultRowData = new GenericRowData(udfOutputType.getFieldCount());
        for (int i = 0; i < udfOutputType.getFieldCount(); i++) {
            reuseNullResultRowData.setField(i, null);
        }
    }

    @Override
    public void openPythonInterpreter() {
        // from pyflink.fn_execution.embedded.operation_utils import
        // create_table_operation_from_proto
        //
        // proto = xxx
        // table_operation = create_table_operation_from_proto(
        //      proto, input_coder_proto, output_coder_proto)
        // table_operation.open()

        interpreter.exec(
                "from pyflink.fn_execution.embedded.operation_utils import create_table_operation_from_proto");

        interpreter.set(
                "input_coder_proto",
                createFlattenRowTypeCoderInfoDescriptorProto(
                                udfInputType, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, false)
                        .toByteArray());

        interpreter.set(
                "output_coder_proto",
                createFlattenRowTypeCoderInfoDescriptorProto(
                                udfOutputType, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, false)
                        .toByteArray());

        interpreter.set(
                "proto",
                ProtoUtils.createUserDefinedFunctionsProto(
                                getRuntimeContext(),
                                new PythonFunctionInfo[] {tableFunction},
                                config.get(PYTHON_METRIC_ENABLED),
                                config.get(PYTHON_PROFILE_ENABLED))
                        .toByteArray());

        interpreter.exec(
                "table_operation = create_table_operation_from_proto("
                        + "proto,"
                        + "input_coder_proto,"
                        + "output_coder_proto)");

        // invoke the open method of table_operation which calls
        // the open method of the user-defined table function.
        interpreter.invokeMethod("table_operation", "open");
    }

    @Override
    public void endInput() {
        if (interpreter != null) {
            // invoke the open method of table_operation which calls
            // the close method of the user-defined table function.
            interpreter.invokeMethod("table_operation", "close");
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData value = element.getValue();

        for (int i = 0; i < userDefinedFunctionInputArgs.length; i++) {
            userDefinedFunctionInputArgs[i] =
                    userDefinedFunctionInputConverters[i].toExternal(value, udfInputOffsets[i]);
        }

        PyIterator udtfResults =
                (PyIterator)
                        interpreter.invokeMethod(
                                "table_operation",
                                "process_element",
                                (Object) (userDefinedFunctionInputArgs));

        if (udtfResults.hasNext()) {
            do {
                Object[] udtfResult = (Object[]) udtfResults.next();
                for (int i = 0; i < udtfResult.length; i++) {
                    reuseResultRowData.setField(
                            i, userDefinedFunctionOutputConverters[i].toInternal(udtfResult[i]));
                }
                rowDataWrapper.collect(reuseJoinedRow.replace(value, reuseResultRowData));
            } while (udtfResults.hasNext());
        } else if (joinType == FlinkJoinType.LEFT) {
            rowDataWrapper.collect(reuseJoinedRow.replace(value, reuseNullResultRowData));
        }
        udtfResults.close();
    }
}
