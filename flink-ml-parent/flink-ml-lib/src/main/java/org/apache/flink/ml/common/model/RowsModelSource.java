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

/** A {@link ModelSource} implementation that reads the model from the memory. */
public class RowsModelSource implements ModelSource {

    /** The rows that hosts the model. */
    private final List<Row> modelRows;

    /**
     * Construct a RowsModelSource with the given rows containing a model.
     *
     * @param modelRows The rows that contains a model.
     */
    public RowsModelSource(List<Row> modelRows) {
        this.modelRows = modelRows;
    }

    @Override
    public List<Row> getModelRows(RuntimeContext runtimeContext) {
        return modelRows;
    }
}
