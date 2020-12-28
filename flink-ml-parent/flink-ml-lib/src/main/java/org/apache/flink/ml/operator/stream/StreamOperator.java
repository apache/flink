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
import org.apache.flink.ml.common.utils.RowTypeDataStream;
import org.apache.flink.ml.common.utils.TableUtil;
import org.apache.flink.ml.operator.AlgoOperator;
import org.apache.flink.ml.operator.stream.source.TableSourceStreamOp;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

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
	 * Get the {@link DataStream} that casted from the output table with the type of {@link Row}.
	 *
	 * @return the casted {@link DataStream}
	 */
	public DataStream<Row> getDataStream() {
		return RowTypeDataStream.fromTable(getMLEnvironmentId(), getOutput());
	}

	@Override
	public StreamOperator select(String clause) {
		return StreamSqlOperators.select(this, clause);
	}

	@Override
	public StreamOperator select(String[] colNames) {
		return select(TableUtil.columnsToSqlClause(colNames));
	}

	@Override
	public StreamOperator as(String clause) {
		return StreamSqlOperators.as(this, clause);
	}

	@Override
	public StreamOperator where(String clause) {
		return StreamSqlOperators.where(this, clause);
	}

	@Override
	public StreamOperator filter(String clause) {
		return StreamSqlOperators.filter(this, clause);
	}

	/**
	 * Union with another <code>StreamOperator</code>, the duplicated records are kept.
	 *
	 * @param rightOp Another <code>StreamOperator</code> to union with.
	 * @return The resulted <code>StreamOperator</code> of the "unionAll" operation.
	 */
	public StreamOperator unionAll(StreamOperator rightOp) {
		return StreamSqlOperators.unionAll(this, rightOp);
	}

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
