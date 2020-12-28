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

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.api.misc.param.WithParams;
import org.apache.flink.ml.params.shared.HasMLEnvironmentId;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;

/**
 * The base class for a stage in a pipeline, either an [[EstimatorBase]] or a [[TransformerBase]].
 *
 * <p>The PipelineStageBase maintains the parameters for the stage. A default constructor is needed
 * in order to restore a pipeline stage.
 *
 * @param <S> The class type of the {@link PipelineStageBase} implementation itself, used by {@link
 *     org.apache.flink.ml.api.misc.param.WithParams} and Cloneable.
 */
public abstract class PipelineStageBase<S extends PipelineStageBase<S>>
        implements WithParams<S>, HasMLEnvironmentId<S>, Cloneable {
    protected Params params;

    public PipelineStageBase() {
        this(null);
    }

    public PipelineStageBase(Params params) {
        if (null == params) {
            this.params = new Params();
        } else {
            this.params = params.clone();
        }
    }

    @Override
    public Params getParams() {
        if (null == this.params) {
            this.params = new Params();
        }
        return this.params;
    }

    @Override
    public S clone() throws CloneNotSupportedException {
        PipelineStageBase result = (PipelineStageBase) super.clone();
        result.params = this.params.clone();
        return (S) result;
    }

    protected static TableEnvironment tableEnvOf(Table table) {
        return ((TableImpl) table).getTableEnvironment();
    }
}
