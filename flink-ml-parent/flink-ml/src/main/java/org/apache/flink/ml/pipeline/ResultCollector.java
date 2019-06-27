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
import org.apache.flink.ml.batchoperator.BatchOperator;
import org.apache.flink.ml.common.AlgoOperator;
import org.apache.flink.ml.params.BaseWithParam;
import org.apache.flink.table.api.Table;

/**
 * Abstract class for the calculator, which need return back computation result by collect method.
 *
 * @param <T> the type of the calculator.
 * @param <R> the type of the returned result.
 */
public abstract class ResultCollector<T extends ResultCollector <T, R>, R>
	implements BaseWithParam <T>, Cloneable {
	protected Params params;
	protected AlgoOperator in;

	public ResultCollector(Table in) {
		this(in, null);
	}

	public ResultCollector(Table in, Params params) {
		this.in = AlgoOperator.sourceFrom(in);
		if (this.in instanceof BatchOperator) {
			if (null == params) {
				this.params = new Params();
			} else {
				this.params = params.clone();
			}
		} else {
			throw new RuntimeException("Only support Batch Data." + in.getClass().getName());
		}
	}

	public abstract R collectResult();

	@Override
	public Params getParams() {
		if (null == this.params) {
			this.params = new Params();
		}
		return this.params;
	}

	@Override
	public T clone() throws CloneNotSupportedException {
		ResultCollector result = (ResultCollector) super.clone();
		result.params = this.params.clone();
		return (T) result;
	}
}
