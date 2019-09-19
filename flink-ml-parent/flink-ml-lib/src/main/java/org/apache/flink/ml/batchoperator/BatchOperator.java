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

package org.apache.flink.ml.batchoperator;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.batchoperator.source.TableSourceBatchOp;
import org.apache.flink.ml.common.AlgoOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.util.Preconditions;

/**
 * Base class of batch algorithm operators extends {@link AlgoOperator}.
 *
 * <p>This class is extended to support the data transmission between the BatchOperator.
 */
public abstract class BatchOperator<T extends BatchOperator<T>> extends AlgoOperator<T> {

	public BatchOperator() {
		super();
	}

	/**
	 * The constructor of BatchOperator with {@link Params}.
	 * @param params the initial Params.
	 */
	public BatchOperator(Params params) {
		super(params);
	}

	/**
	 * Abbreviation of {@link #linkTo(BatchOperator)}.
	 */
	public <B extends BatchOperator<?>> B link(B next) {
		return linkTo(next);
	}

	/**
	 * Link to another {@link BatchOperator}.
	 *
	 * <p>Link the <code>next</code> to a new BatchOperator using this as input.
	 *
	 * <p>For example:
	 *
	 * <pre>
	 * {@code
	 * BatchOperator a = ...;
	 * BatchOperator b = ...;
	 *
	 * BatchOperator c = a.linkTo(b)
	 * }
	 * </pre>
	 *
	 * <p>the <code>c</code> in upper code indict the linked
	 * <code>b</code> which use <code>a</code> as input.
	 *
	 * @see #linkFrom(BatchOperator[])
	 *
	 * @param next the linked BatchOperator
	 * @param <B> type of BatchOperator returned
	 * @return the linked next
	 */
	public <B extends BatchOperator<?>> B linkTo(B next) {
		next.linkFrom(this);
		return next;
	}

	/**
	 * Link from others {@link BatchOperator}.
	 *
	 * <p>Link this object to a new BatchOperator using the inputs.
	 *
	 * <p>For example:
	 *
	 * <pre>
	 * {@code
	 * BatchOperator a = ...;
	 * BatchOperator b = ...;
	 * BatchOperator c = ...;
	 *
	 * BatchOperator d = c.linkFrom(a, b)
	 * }
	 * </pre>
	 *
	 * <p>the <code>d</code> in upper code indict the linked
	 * <code>c</code> which use a and b as its inputs.
	 *
	 * <p>note: It is not recommended to linkFrom itself or link the same group inputs twice.
	 *
	 * @param inputs the linked inputs
	 * @return the linked this object
	 */
	public abstract T linkFrom(BatchOperator<?>... inputs);

	/**
	 * create a new BatchOperator from table.
	 * @param table the input table
	 * @return the new BatchOperator
	 */
	public static BatchOperator<?> sourceFrom(Table table) {
		return new TableSourceBatchOp(table);
	}

	protected void checkOpSize(int size, BatchOperator<?>... inputs) {
		Preconditions.checkNotNull(inputs, "Operators should not be null.");
		Preconditions.checkState(inputs.length == size, "The size of operators should be equal to "
			+ size + ", current: " + inputs.length);
	}

	protected void checkRequiredOpSize(int size, BatchOperator<?>... inputs) {
		Preconditions.checkNotNull(inputs, "Operators should not be null.");
		Preconditions.checkState(inputs.length >= size, "The size of operators should be equal or greater than "
			+ size + ", current: " + inputs.length);
	}

	protected BatchOperator<?> checkAndGetFirst(BatchOperator<?> ... inputs) {
		checkOpSize(1, inputs);
		return inputs[0];
	}
}
