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

package org.apache.flink.optimizer.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.NoOpFunction;
import org.apache.flink.api.common.operators.BinaryOperatorInformation;
import org.apache.flink.api.common.operators.DualInputOperator;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.List;

public class NoOpBinaryUdfOp<OUT> extends DualInputOperator<OUT, OUT, OUT, NoOpFunction> {

    public NoOpBinaryUdfOp(TypeInformation<OUT> type) {
        super(
                new UserCodeClassWrapper<NoOpFunction>(NoOpFunction.class),
                new BinaryOperatorInformation<OUT, OUT, OUT>(type, type, type),
                "NoContract");
    }

    @Override
    protected List<OUT> executeOnCollections(
            List<OUT> inputData1,
            List<OUT> inputData2,
            RuntimeContext runtimeContext,
            ExecutionConfig executionConfig) {
        throw new UnsupportedOperationException();
    }
}
