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

package org.apache.flink.ml.streamoperator;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.AlgoOperator;
import org.apache.flink.ml.streamoperator.source.TableSourceStreamOp;
import org.apache.flink.table.api.Table;
import org.apache.flink.util.Preconditions;

/**
 * Base class of stream algorithm operators.
 *
 * <p>This class is extended to support the data transmission between the StreamOperators.
 */
public abstract class StreamOperator<T extends StreamOperator<T>> extends AlgoOperator<T> {

	public StreamOperator() {
		super();
	}

	/**
	 * The constructor of StreamOperator with {@link Params}.
	 *
	 * @param params the initial Params.
	 */
	public StreamOperator(Params params) {
		super(params);
	}

	/**
	 * Link to another {@link StreamOperator}.
	 *
	 * <p>Link the <code>next</code> to a new StreamOperator using this as input.
	 *
	 * <p>For example:
	 *
	 * <pre>
	 * {@code
	 * StreamOperator a = ...;
	 * StreamOperator b = ...;
	 *
	 * StreamOperator c = a.link(b)
	 * }
	 * </pre>
	 *
	 * <p>the <code>c</code> in upper code indict the linked
	 * <code>b</code> which use <code>a</code> as input.
	 *
	 * @param next the linked StreamOperator
	 * @param <S>  type of StreamOpearator returned
	 * @return the linked next
	 * @see #linkFrom(StreamOperator[])
	 */
	public <S extends StreamOperator<?>> S link(S next) {
		next.linkFrom(this);
		return next;
	}

	/**
	 * Link from others {@link StreamOperator}.
	 *
	 * <p>Link this object to StreamOperator using the StreamOperators as its input.
	 *
	 * <p>For example:
	 *
	 * <pre>
	 * {@code
	 * StreamOperator a = ...;
	 * StreamOperator b = ...;
	 * StreamOperator c = ...;
	 *
	 * StreamOperator d = c.linkFrom(a, b)
	 * }
	 * </pre>
	 *
	 * <p>the <code>d</code> in upper code is the linked
	 * <code>c</code> which use a and b as its inputs.
	 *
	 * <p>note: It is not recommended to linkFrom itself or linkFrom the same group inputs twice.
	 *
	 * @param inputs the linked inputs
	 * @return the linked this object
	 */
	public abstract T linkFrom(StreamOperator<?>... inputs);

	/**
	 * create a new StreamOperator from table.
	 *
	 * @param table the input table
	 * @return the new StreamOperator
	 */
	public static StreamOperator<?> sourceFrom(Table table) {
		return new TableSourceStreamOp(table);
	}

	protected void checkOpSize(int size, StreamOperator<?>... inputs) {
		Preconditions.checkNotNull(inputs, "Operators should not be null.");
		Preconditions.checkState(inputs.length == size, "The size of operators should be equal to "
			+ size + ", current: " + inputs.length);
	}

	protected void checkRequiredOpSize(int size, StreamOperator<?>... inputs) {
		Preconditions.checkNotNull(inputs, "Operators should not be null.");
		Preconditions.checkState(inputs.length >= size, "The size of operators should be equal or greater than "
			+ size + ", current: " + inputs.length);
	}

	protected StreamOperator<?> checkAndGetFirst(StreamOperator<?>... inputs) {
		checkOpSize(1, inputs);
		return inputs[0];
	}
}
