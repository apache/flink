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

package org.apache.flink.ml.operator.stream;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.operator.AlgoOperator;
import org.apache.flink.ml.operator.stream.source.TableSourceStreamOp;
import org.apache.flink.table.api.Table;

/**
 * Base class of stream algorithm operators.
 *
 * <p>This class extends {@link AlgoOperator} to support data transmission between StreamOperator.
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
	 * <p>Link the <code>next</code> StreamOperator using this StreamOperator as its input.
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
	 * <p>The StreamOperator <code>c</code> in the above code
	 * is the same instance as <code>b</code> which takes
	 * <code>a</code> as its input.
	 * Note that StreamOperator <code>b</code> will be changed
	 * to link from StreamOperator <code>a</code>.
	 *
	 * @param next the linked StreamOperator
	 * @param <S>  type of StreamOperator returned
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
	 * <p>The <code>d</code> in the above code is the same
	 * instance as StreamOperator <code>c</code> which takes
	 * both <code>a</code> and <code>b</code> as its input.
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
	public static StreamOperator<?> fromTable(Table table) {
		return new TableSourceStreamOp(table);
	}

	protected static StreamOperator<?> checkAndGetFirst(StreamOperator<?>... inputs) {
		checkOpSize(1, inputs);
		return inputs[0];
	}
}
