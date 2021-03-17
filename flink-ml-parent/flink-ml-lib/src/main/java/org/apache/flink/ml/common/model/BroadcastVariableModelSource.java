/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.model;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.types.Row;

import java.util.List;

/** A {@link ModelSource} implementation that reads the model from the broadcast variable. */
public class BroadcastVariableModelSource implements ModelSource {

    /** The name of the broadcast variable that hosts the model. */
    private final String modelVariableName;

    /**
     * Construct a BroadcastVariableModelSource.
     *
     * @param modelVariableName The name of the broadcast variable that hosts a
     *     BroadcastVariableModelSource.
     */
    public BroadcastVariableModelSource(String modelVariableName) {
        this.modelVariableName = modelVariableName;
    }

    @Override
    public List<Row> getModelRows(RuntimeContext runtimeContext) {
        return runtimeContext.getBroadcastVariable(modelVariableName);
    }
}
