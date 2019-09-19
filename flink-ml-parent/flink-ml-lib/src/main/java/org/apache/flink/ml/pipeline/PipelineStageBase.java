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

/**
 * The base class for a stage in a pipeline, either an [[Estimator]] or a [[Transformer]].
 *
 * <p>Each pipeline stage is with parameters, and requires a public empty constructor for
 * restoration in Pipeline. It hold a {@link Params} as its member, thus the subclasses
 * could do not care about {@link WithParams#getParams()}
 *
 * @param <S> The class type of the {@link PipelineStageBase} implementation itself, used by {@link
 *            org.apache.flink.ml.api.misc.param.WithParams} and Cloneable.
 */
public abstract class PipelineStageBase<S extends PipelineStageBase<S>>
	implements WithParams <S>, HasMLEnvironmentId<S>, Cloneable {
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
}
