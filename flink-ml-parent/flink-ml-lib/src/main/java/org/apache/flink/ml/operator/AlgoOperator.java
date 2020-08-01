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

package org.apache.flink.ml.operator;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.api.misc.param.WithParams;
import org.apache.flink.ml.params.shared.HasMLEnvironmentId;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Base class for algorithm operators.
 *
 * <p>Base class for the algorithm operators. It hosts the parameters and output
 * tables of an algorithm operator. Each AlgoOperator may have one or more output tables.
 * One of the output table is the primary output table which can be obtained by calling
 * {@link #getOutput}. The other output tables are side output tables that can be obtained
 * by calling {@link #getSideOutputs()}.
 *
 * <p>The input of an AlgoOperator is defined in the subclasses of the AlgoOperator.
 *
 * @param <T> The class type of the {@link AlgoOperator} implementation itself
 */
public abstract class AlgoOperator<T extends AlgoOperator<T>>
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
	 *
	 * <p>This constructor is especially useful when users want to set parameters
	 * for the algorithm operators. For example:
	 * SplitBatchOp is widely used in ML data pre-processing,
	 * which splits one dataset into two dataset: training set and validation set.
	 * It is very convenient for us to write code like this:
	 * <pre>
	 * {@code
	 * new SplitBatchOp().setSplitRatio(0.9)
	 * }
	 * </pre>
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
		return this.params;
	}

	/**
	 * Returns the table held by operator.
	 */
	public Table getOutput() {
		return this.output;
	}

	/**
	 * Returns the side outputs.
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
	 * Returns the column names of the output table.
	 */
	public String[] getColNames() {
		return getSchema().getFieldNames();
	}

	/**
	 * Returns the column types of the output table.
	 */
	public TypeInformation<?>[] getColTypes() {
		return getSchema().getFieldTypes();
	}

	/**
	 * Get the column names of the specified side-output table.
	 *
	 * @param index the index of the table.
	 * @return the column types of the table.
	 */
	public String[] getSideOutputColNames(int index) {
		checkSideOutputAccessibility(index);

		return sideOutputs[index].getSchema().getFieldNames();
	}

	/**
	 * Get the column types of the specified side-output table.
	 *
	 * @param index the index of the table.
	 * @return the column types of the table.
	 */
	public TypeInformation<?>[] getSideOutputColTypes(int index) {
		checkSideOutputAccessibility(index);

		return sideOutputs[index].getSchema().getFieldTypes();
	}

	/**
	 * Returns the schema of the output table.
	 */
	public TableSchema getSchema() {
		return this.getOutput().getSchema();
	}

	@Override
	public String toString() {
		return getOutput().toString();
	}

	protected static void checkOpSize(int size, AlgoOperator<?>... inputs) {
		Preconditions.checkNotNull(inputs, "Operators should not be null.");
		Preconditions.checkState(inputs.length == size, "The size of operators should be equal to "
			+ size + ", current: " + inputs.length);
	}

	protected static void checkMinOpSize(int size, AlgoOperator<?>... inputs) {
		Preconditions.checkNotNull(inputs, "Operators should not be null.");
		Preconditions.checkState(inputs.length >= size, "The size of operators should be equal or greater than "
			+ size + ", current: " + inputs.length);
	}

	private void checkSideOutputAccessibility(int index) {
		Preconditions.checkNotNull(sideOutputs,
			"There is not side-outputs in this AlgoOperator.");
		Preconditions.checkState(index >= 0 && index < sideOutputs.length,
			String.format("The index(%s) of side-outputs is out of bound.", index));
		Preconditions.checkNotNull(sideOutputs[index],
			String.format("The %snd of side-outputs is null. Maybe the operator has not been linked.", index));
	}
}
