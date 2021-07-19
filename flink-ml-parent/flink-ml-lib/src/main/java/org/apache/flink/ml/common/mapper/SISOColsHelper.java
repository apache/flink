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

package org.apache.flink.ml.common.mapper;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.utils.OutputColsHelper;
import org.apache.flink.ml.common.utils.TableUtil;
import org.apache.flink.ml.params.mapper.SISOFlatMapperParams;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.FunctionWithException;

import java.io.Serializable;

/**
 * A helper which helps to handle SISO mappers and flatmappers with a {@link SISOColsHelper} inside.
 */
class SISOColsHelper implements Serializable {

	private OutputColsHelper outputColsHelper;

	/**
	 * Index of input column in a row.
	 */
	private int colIndex;

	/**
	 * Create a SISOColsHelper object.
	 *
	 * <p>The selected column must appear in input data, or an  {@code IllegalArgumentException} will be thrown.
	 * If the output columns are not specified via params, it will be set to same as selected ones.
	 *
	 * @param dataSchema TableSchema of input rows.
	 * @param outputType Type of the single output column.
	 * @param params     Params to retrieve selected/output/reserved columns.
	 */
	SISOColsHelper(TableSchema dataSchema, TypeInformation<?> outputType, Params params) {
		String selectedColName = params.get(SISOFlatMapperParams.SELECTED_COL);
		String outputColName = params.get(SISOFlatMapperParams.OUTPUT_COL);
		String[] keepColNames = params.get(SISOFlatMapperParams.RESERVED_COLS);

		this.colIndex = TableUtil.findColIndex(dataSchema.getFieldNames(), selectedColName);
		if (this.colIndex < 0) {
			throw new IllegalArgumentException("Can't find column " + selectedColName);
		}
		if (outputColName == null) {
			outputColName = selectedColName;
		}
		this.outputColsHelper = new OutputColsHelper(dataSchema, outputColName, outputType, keepColNames);
	}

	/**
	 * Get output schema by {@link OutputColsHelper}.
	 *
	 * @return The output schema.
	 */
	TableSchema getOutputSchema() {
		return this.outputColsHelper.getResultSchema();
	}

	/**
	 * Handle mappers.
	 *
	 * @param input input row.
	 * @param func  the function mapping from single column to single column.
	 * @return the result row with un-effected columns set properly.
	 * @throws Exception
	 */
	Row handleMap(Row input, FunctionWithException<Object, Object, Exception> func) throws Exception {
		Object output = func.apply(input.getField(this.colIndex));
		return this.outputColsHelper.getResultRow(input, Row.of(output));
	}

	/**
	 * Handle flat mappers.
	 *
	 * @param input     input row
	 * @param collector collector to accept result rows.
	 * @param func      the function mapping from single column to zero or more values.
	 * @throws Exception
	 */
	void handleFlatMap(
		Row input, Collector<Row> collector,
		BiConsumerWithException<Object, Collector<Object>, Exception> func) throws Exception {
		CollectorWrapper collectorWrapper = new CollectorWrapper(collector, input);
		func.accept(input.getField(this.colIndex), collectorWrapper);
	}

	/**
	 * A wrapped {@link Collector} which accepts a single column but feeds a row to underlying
	 * proxy {@link Collector} which accept {@link Row}. The row is create from the the accepted
	 * column object, with other columns handled property by {@link OutputColsHelper}.
	 */
	private class CollectorWrapper implements Collector<Object> {

		private final Collector<Row> proxy;
		private final Row row;

		CollectorWrapper(Collector<Row> proxy, Row row) {
			this.proxy = proxy;
			this.row = row;
		}

		@Override
		public void collect(Object record) {
			Row output = outputColsHelper.getResultRow(row, Row.of(record));
			this.proxy.collect(output);
		}

		@Override
		public void close() {
			this.proxy.close();
		}
	}

}
