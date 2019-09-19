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
import org.apache.flink.ml.params.shared.HasMLEnvironmentId;
import org.apache.flink.ml.streamoperator.source.TableSourceStreamOp;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.io.Serializable;

/**
 * Base class for algorithm operators.
 *
 * <p>Hold a Table as its output.
 *
 * @param <T> The class type of the {@link AlgoOperator} implementation itself
 */
public abstract class AlgoOperator<T extends AlgoOperator <T>>
	implements WithParams<T>, HasMLEnvironmentId<T>, Serializable {

	/**
	 * Params for algorithms.
	 */
	private Params params;

	/**
	 * The table held by operator.
	 */
	private Table output = null;

	/**
	 * The side outputs of operator that be similar to the stream's side outputs.
	 */
	private Table[] sideOutputs = null;

	/**
	 * Construct the operator with empty Params.
	 */
	protected AlgoOperator() {
		this(null);
	}

	/**
	 * Construct the operator with the initial Params.
	 */
	protected AlgoOperator(Params params) {
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

	/**
	 * Returns the table held by operator.
	 *
	 * @return the table
	 */
	public Table getOutput() {
		return this.output;
	}

	/**
	 * Returns the side outputs.
	 *
	 * @return the side outputs.
	 */
	public Table[] getSideOutputs() {
		return this.sideOutputs;
	}

	/**
	 * Set the side outputs.
	 *
	 * @param sideOutputs the side outputs set the operator.
	 */
	protected void setSideOutputs(Table[] sideOutputs) {
		this.sideOutputs = sideOutputs;
	}

	/**
	 * Set the table held by operator.
	 *
	 * @param output the output table.
	 */
	protected void setOutput(Table output) {
		this.output = output;
	}

	/**
	 * Get the column names of the output table.
	 *
	 * @return the column names.
	 */
	public String[] getColNames() {
		return getSchema().getFieldNames();
	}

	/**
	 * Get the column types of the output table.
	 *
	 * @return the column types.
	 */
	public TypeInformation <?>[] getColTypes() {
		return getSchema().getFieldTypes();
	}

	/**
	 * Get the schema of the output table.
	 *
	 * @return the schema.
	 */
	public TableSchema getSchema() {
		return this.getOutput().getSchema();
	}

	/**
	 * Returns the name of output table.
	 *
	 * @return the name of output table.
	 */
	@Override
	public String toString() {
		return getOutput().toString();
	}

	/**
	 * create a new AlgoOperator from table.
	 * @param table the input table
	 * @return the new AlgoOperator
	 */
	public static AlgoOperator<?> sourceFrom(Table table) {
		if (((TableImpl) table).getTableEnvironment() instanceof StreamTableEnvironment) {
			return new TableSourceStreamOp(table);
		} else {
			return new TableSourceBatchOp(table);
		}
	}
}
