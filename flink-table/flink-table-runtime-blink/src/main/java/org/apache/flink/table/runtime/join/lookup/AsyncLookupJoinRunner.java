/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.join.lookup;

import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.DataFormatConverters;
import org.apache.flink.table.dataformat.DataFormatConverters.RowConverter;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.generated.GeneratedFunction;
import org.apache.flink.table.generated.GeneratedResultFuture;
import org.apache.flink.table.runtime.collector.TableFunctionResultFuture;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

/**
 * The async join runner to lookup the dimension table.
 */
public class AsyncLookupJoinRunner extends RichAsyncFunction<BaseRow, BaseRow> {
	private static final long serialVersionUID = -6664660022391632480L;

	private final GeneratedFunction<AsyncFunction<BaseRow, Object>> generatedFetcher;
	private final GeneratedResultFuture<TableFunctionResultFuture<BaseRow>> generatedResultFuture;
	private final boolean isLeftOuterJoin;
	private final int asyncBufferCapacity;
	private final TypeInformation<?> fetcherReturnType;
	private final BaseRowTypeInfo rightRowTypeInfo;

	private transient AsyncFunction<BaseRow, Object> fetcher;

	/**
	 * Buffers {@link ResultFuture} to avoid newInstance cost when processing elements every time.
	 * We use {@link BlockingQueue} to make sure the head {@link ResultFuture}s are available.
	 */
	private transient BlockingQueue<JoinedRowResultFuture> resultFutureBuffer;
	/**
	 * A Collection contains all ResultFutures in the runner which is used to invoke
	 * {@code close()} on every ResultFuture. {@link #resultFutureBuffer} may not
	 * contain all the ResultFutures because ResultFutures will be polled from the buffer when
	 * processing.
	 */
	private transient List<JoinedRowResultFuture> allResultFutures;

	public AsyncLookupJoinRunner(
			GeneratedFunction<AsyncFunction<BaseRow, Object>> generatedFetcher,
			GeneratedResultFuture<TableFunctionResultFuture<BaseRow>> generatedResultFuture,
			TypeInformation<?> fetcherReturnType,
			BaseRowTypeInfo rightRowTypeInfo,
			boolean isLeftOuterJoin,
			int asyncBufferCapacity) {
		this.generatedFetcher = generatedFetcher;
		this.generatedResultFuture = generatedResultFuture;
		this.isLeftOuterJoin = isLeftOuterJoin;
		this.asyncBufferCapacity = asyncBufferCapacity;
		this.fetcherReturnType = fetcherReturnType;
		this.rightRowTypeInfo = rightRowTypeInfo;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.fetcher = generatedFetcher.newInstance(getRuntimeContext().getUserCodeClassLoader());
		FunctionUtils.setFunctionRuntimeContext(fetcher, getRuntimeContext());
		FunctionUtils.openFunction(fetcher, parameters);

		// try to compile the generated ResultFuture, fail fast if the code is corrupt.
		generatedResultFuture.compile(getRuntimeContext().getUserCodeClassLoader());

		// row converter is stateless which is thread-safe
		RowConverter rowConverter;
		if (fetcherReturnType instanceof RowTypeInfo) {
			rowConverter = (RowConverter) DataFormatConverters.getConverterForDataType(
					fromLegacyInfoToDataType(fetcherReturnType));
		} else if (fetcherReturnType instanceof BaseRowTypeInfo) {
			rowConverter = null;
		} else {
			throw new IllegalStateException("This should never happen, " +
				"currently fetcherReturnType can only be BaseRowTypeInfo or RowTypeInfo");
		}

		// asyncBufferCapacity + 1 as the queue size in order to avoid
		// blocking on the queue when taking a collector.
		this.resultFutureBuffer = new ArrayBlockingQueue<>(asyncBufferCapacity + 1);
		this.allResultFutures = new ArrayList<>();
		for (int i = 0; i < asyncBufferCapacity + 1; i++) {
			JoinedRowResultFuture rf = new JoinedRowResultFuture(
				resultFutureBuffer,
				createFetcherResultFuture(parameters),
				rowConverter,
				isLeftOuterJoin,
				rightRowTypeInfo.getArity());
			// add will throw exception immediately if the queue is full which should never happen
			resultFutureBuffer.add(rf);
			allResultFutures.add(rf);
		}
	}

	@Override
	public void asyncInvoke(BaseRow input, ResultFuture<BaseRow> resultFuture) throws Exception {
		JoinedRowResultFuture outResultFuture = resultFutureBuffer.take();
		// the input row is copied when object reuse in AsyncWaitOperator
		outResultFuture.reset(input, resultFuture);

		// fetcher has copied the input field when object reuse is enabled
		fetcher.asyncInvoke(input, outResultFuture);
	}

	public TableFunctionResultFuture<BaseRow> createFetcherResultFuture(Configuration parameters) throws Exception {
		TableFunctionResultFuture<BaseRow> resultFuture = generatedResultFuture.newInstance(
			getRuntimeContext().getUserCodeClassLoader());
		FunctionUtils.setFunctionRuntimeContext(resultFuture, getRuntimeContext());
		FunctionUtils.openFunction(resultFuture, parameters);
		return resultFuture;
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (fetcher != null) {
			FunctionUtils.closeFunction(fetcher);
		}
		for (JoinedRowResultFuture rf : allResultFutures) {
			rf.close();
		}
	}

	/**
	 * The {@link JoinedRowResultFuture} is used to combine left {@link BaseRow} and
	 * right {@link BaseRow} into {@link JoinedRow}.
	 *
	 * <p>There are 3 phases in this collector.
	 *
	 * <ol>
	 *     <li>accept lookup function return result and convert it into BaseRow, call it right result</li>
	 *     <li>project & filter the right result if there is a calc on the temporal table,
	 *     see {@link AsyncLookupJoinWithCalcRunner#createFetcherResultFuture(Configuration)}</li>
	 *     <li>filter the result if a join condition exist,
	 *     see {@link AsyncLookupJoinRunner#createFetcherResultFuture(Configuration)}</li>
	 *     <li>combine left input and the right result into a JoinedRow, call it join result</li>
	 * </ol>
	 *
	 * <p>TODO: code generate a whole JoinedRowResultFuture in the future
	 */
	private static final class JoinedRowResultFuture implements ResultFuture<Object> {

		private final BlockingQueue<JoinedRowResultFuture> resultFutureBuffer;
		private final TableFunctionResultFuture<BaseRow> joinConditionResultFuture;
		private final RowConverter rowConverter;
		private final boolean isLeftOuterJoin;

		private final DelegateResultFuture delegate;
		private final GenericRow nullRow;

		private BaseRow leftRow;
		private ResultFuture<BaseRow> realOutput;

		private JoinedRowResultFuture(
				BlockingQueue<JoinedRowResultFuture> resultFutureBuffer,
				TableFunctionResultFuture<BaseRow> joinConditionResultFuture,
				@Nullable RowConverter rowConverter,
				boolean isLeftOuterJoin,
				int rightArity) {
			this.resultFutureBuffer = resultFutureBuffer;
			this.joinConditionResultFuture = joinConditionResultFuture;
			this.rowConverter = rowConverter;
			this.isLeftOuterJoin = isLeftOuterJoin;
			this.delegate = new DelegateResultFuture();
			this.nullRow = new GenericRow(rightArity);
		}

		public void reset(BaseRow row, ResultFuture<BaseRow> realOutput) {
			this.realOutput = realOutput;
			this.leftRow = row;
			joinConditionResultFuture.setInput(row);
			joinConditionResultFuture.setResultFuture(delegate);
			delegate.reset();
		}

		@Override
		public void complete(Collection<Object> result) {
			Collection<BaseRow> baseRows;
			if (rowConverter == null) {
				// result is BaseRow Collection
				//noinspection unchecked
				baseRows = (Collection) result;
			} else {
				baseRows = new ArrayList<>(result.size());
				for (Object element : result) {
					Row row = (Row) element;
					baseRows.add(rowConverter.toInternal(row));
				}
			}

			// call condition collector first,
			// the filtered result will be routed to the delegateCollector
			try {
				joinConditionResultFuture.complete(baseRows);
			} catch (Throwable t) {
				// we should catch the exception here to let the framework know
				completeExceptionally(t);
				return;
			}

			Collection<BaseRow> rightRows = delegate.collection;
			if (rightRows == null || rightRows.isEmpty()) {
				if (isLeftOuterJoin) {
					BaseRow outRow = new JoinedRow(leftRow, nullRow);
					outRow.setHeader(leftRow.getHeader());
					realOutput.complete(Collections.singleton(outRow));
				} else {
					realOutput.complete(Collections.emptyList());
				}
			} else {
				List<BaseRow> outRows = new ArrayList<>();
				for (BaseRow rightRow : rightRows) {
					BaseRow outRow = new JoinedRow(leftRow, rightRow);
					outRow.setHeader(leftRow.getHeader());
					outRows.add(outRow);
				}
				realOutput.complete(outRows);
			}
			try {
				// put this collector to the queue to avoid this collector is used
				// again before outRows in the collector is not consumed.
				resultFutureBuffer.put(this);
			} catch (InterruptedException e) {
				completeExceptionally(e);
			}
		}

		@Override
		public void completeExceptionally(Throwable error) {
			realOutput.completeExceptionally(error);
		}

		public void close() throws Exception {
			joinConditionResultFuture.close();
		}

		private final class DelegateResultFuture implements ResultFuture<BaseRow> {

			private Collection<BaseRow> collection;

			public void reset() {
				this.collection = null;
			}

			@Override
			public void complete(Collection<BaseRow> result) {
				this.collection = result;
			}

			@Override
			public void completeExceptionally(Throwable error) {
				JoinedRowResultFuture.this.completeExceptionally(error);
			}
		}
	}
}
