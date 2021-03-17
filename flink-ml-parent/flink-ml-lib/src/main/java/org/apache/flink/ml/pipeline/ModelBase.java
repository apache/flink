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

package org.apache.flink.ml.pipeline;

import org.apache.flink.ml.api.core.Model;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

/**
 * The base class for a machine learning model.
 *
 * @param <M> The class type of the {@link ModelBase} implementation itself
 */
public abstract class ModelBase<M extends ModelBase<M>> extends TransformerBase<M>
        implements Model<M> {

    protected Table modelData;

    public ModelBase() {
        super();
    }

    public ModelBase(Params params) {
        super(params);
    }

    /**
     * Get model data as Table representation.
     *
     * @return the Table
     */
    public Table getModelData() {
        return this.modelData;
    }

    /**
     * Set the model data using the Table.
     *
     * @param modelData the Table.
     * @return {@link ModelBase} itself
     */
    public M setModelData(Table modelData) {
        this.modelData = modelData;
        return (M) this;
    }

    @Override
    public M clone() throws CloneNotSupportedException {
        return (M) super.clone().setModelData(this.modelData);
    }
}
