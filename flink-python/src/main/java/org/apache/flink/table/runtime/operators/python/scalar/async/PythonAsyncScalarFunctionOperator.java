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

package org.apache.flink.table.runtime.operators.python.scalar.async;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.operators.python.scalar.PythonScalarFunctionOperator;
import org.apache.flink.table.types.logical.RowType;

/** The Python {@link AsyncScalarFunction} operator for Table API. */
@Internal
public class PythonAsyncScalarFunctionOperator extends PythonScalarFunctionOperator {

    private static final long serialVersionUID = 1L;

    private static final String ASYNC_SCALAR_FUNCTION_URN =
            "flink:transform:async_scalar_function:v1";

    /**
     * The maximum number of async operations that can be in-flight at the same time. This controls
     * the buffer capacity for async execution.
     */
    private final int asyncMaxConcurrentOperations;

    /** The timeout in milliseconds for async operations. */
    private final long asyncTimeout;

    /** Whether retry is enabled for async operations. */
    private final boolean asyncRetryEnabled;

    /** Maximum number of retry attempts. */
    private final int asyncRetryMaxAttempts;

    /** Delay between retries in milliseconds. */
    private final long asyncRetryDelayMs;

    public PythonAsyncScalarFunctionOperator(
            Configuration config,
            PythonFunctionInfo[] scalarFunctions,
            RowType inputType,
            RowType udfInputType,
            RowType udfOutputType,
            GeneratedProjection udfInputGeneratedProjection,
            GeneratedProjection forwardedFieldGeneratedProjection,
            int asyncBufferCapacity,
            long asyncTimeout,
            boolean asyncRetryEnabled,
            int asyncRetryMaxAttempts,
            long asyncRetryDelayMs) {
        super(
                config,
                scalarFunctions,
                inputType,
                udfInputType,
                udfOutputType,
                udfInputGeneratedProjection,
                forwardedFieldGeneratedProjection);
        this.asyncMaxConcurrentOperations = asyncBufferCapacity;
        this.asyncTimeout = asyncTimeout;
        this.asyncRetryEnabled = asyncRetryEnabled;
        this.asyncRetryMaxAttempts = asyncRetryMaxAttempts;
        this.asyncRetryDelayMs = asyncRetryDelayMs;
    }

    @Override
    public String getFunctionUrn() {
        return ASYNC_SCALAR_FUNCTION_URN;
    }

    @Override
    public FlinkFnApi.UserDefinedFunctions createUserDefinedFunctionsProto() {
        // Create the base proto with scalar functions
        FlinkFnApi.UserDefinedFunctions.Builder builder =
                super.createUserDefinedFunctionsProto().toBuilder();

        // Add async-specific configurations
        builder.setAsyncOptions(
                FlinkFnApi.AsyncOptions.newBuilder()
                        .setMaxConcurrentOperations(asyncMaxConcurrentOperations)
                        .setTimeoutMs(asyncTimeout)
                        .setRetryEnabled(asyncRetryEnabled)
                        .setRetryMaxAttempts(asyncRetryMaxAttempts)
                        .setRetryDelayMs(asyncRetryDelayMs));

        return builder.build();
    }
}
