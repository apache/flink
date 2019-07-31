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

package org.apache.flink.ml.common;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.api.misc.param.WithParams;
import org.apache.flink.ml.batchoperator.source.TableSourceBatchOp;
import org.apache.flink.ml.streamoperator.source.TableSourceStreamOp;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.io.Serializable;

/**
 * Base class for algorithm operators.
 * @param <T> The class type of the {@link AlgoOperator} implementation itself
 */
public abstract class AlgoOperator<T extends AlgoOperator <T>> implements WithParams<T>, Serializable {

	protected Params params;

	protected Table output = null;

	protected Table[] sideOutputs = null;

	protected AlgoOperator() {
		this(null);
	}

	protected AlgoOperator(Params params) {
		if (null == params) {
			this.params = new Params();
		} else {
			this.params = params.clone();
		}
	}

	public static AlgoOperator sourceFrom(Table table) {
		if (((TableImpl) table).getTableEnvironment() instanceof StreamTableEnvironment) {
			return new TableSourceStreamOp(table);
		} else {
			return new TableSourceBatchOp(table);
		}
	}

	@Override
	public Params getParams() {
		if (null == this.params) {
			this.params = new Params();
		}
		return this.params;
	}

	public Table getOutput() {
		return this.output;
	}

	protected void setOutput(Table output) {
		this.output = output;
	}

	public Table[] getSideOutputs() {
		return this.sideOutputs;
	}

	public String[] getColNames() {
		return getSchema().getFieldNames();
	}

	public TypeInformation<?>[] getColTypes() {
		return getSchema().getFieldTypes();
	}

	public TableSchema getSchema() {
		return getOutput().getSchema();
	}

}
