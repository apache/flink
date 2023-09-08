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

package org.apache.flink.cep.pattern.conditions;

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

/**
 * Rich variant of the {@link IterativeCondition}. As a {@link RichFunction}, it gives access to the
 * {@link org.apache.flink.api.common.functions.RuntimeContext} and provides setup and teardown
 * methods: {@link RichFunction#open(OpenContext)} and {@link RichFunction#close()}.
 */
public abstract class RichIterativeCondition<T> extends IterativeCondition<T>
        implements RichFunction {

    private static final long serialVersionUID = 1L;

    // --------------------------------------------------------------------------------------------
    //  Runtime context access
    // --------------------------------------------------------------------------------------------

    private transient RuntimeContext runtimeContext;

    @Override
    public void setRuntimeContext(RuntimeContext runtimeContext) {
        Preconditions.checkNotNull(runtimeContext);
        this.runtimeContext = runtimeContext;
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        if (this.runtimeContext != null) {
            return this.runtimeContext;
        } else {
            throw new IllegalStateException("The runtime context has not been initialized.");
        }
    }

    @Override
    public IterationRuntimeContext getIterationRuntimeContext() {
        throw new UnsupportedOperationException(
                "Not support to get the IterationRuntimeContext in IterativeCondition.");
    }

    // --------------------------------------------------------------------------------------------
    //  Default life cycle methods
    // --------------------------------------------------------------------------------------------

    @Override
    public void open(OpenContext openContext) throws Exception {}

    @Override
    public void open(Configuration parameters) throws Exception {
        throw new UnsupportedOperationException(
                "This method is deprecated and shouldn't be invoked. Please use open(OpenContext) instead.");
    }

    @Override
    public void close() throws Exception {}
}
