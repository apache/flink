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
import org.apache.flink.ml.params.mode.SISOMapperParams;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

/**
 * Abstract class for mappers with Single Input column and Single Output column(SISO).
 * Operations that produce zero, one or more Row type result data per Row type data
 * can also use the {@link SISOFlatMapper}.
 */
public abstract class SISOMapper extends Mapper {
	private final OutputColsHelper outputColsHelper;
	private final int colIndex;

	/**
	 * Constructor.
	 *
	 * @param dataSchema input tableSchema
	 * @param params input parameters.
	 */
	public SISOMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		String inputColName = this.params.get(SISOMapperParams.SELECTED_COL);
		this.colIndex = TableUtil.findColIndex(dataSchema.getFieldNames(), inputColName);
		if (this.colIndex < 0) {
			throw new RuntimeException("Can not find column " + inputColName);
		}
		String outputColName = null;
		if (this.params.contains(SISOMapperParams.OUTPUT_COL)) {
			outputColName = params.get(SISOMapperParams.OUTPUT_COL);
		}
		if (null == outputColName) {
			outputColName = inputColName;
		}
		String[] keepColNames = null;
		if (this.params.contains(SISOMapperParams.RESERVED_COLS)) {
			keepColNames = this.params.get(SISOMapperParams.RESERVED_COLS);
		}
		this.outputColsHelper = new OutputColsHelper(dataSchema, outputColName, initOutputColType(), keepColNames);
	}

	/**
	 * Determine the return type of the {@link SISOMapper#map(Object)}.
	 *
	 * @return the outputColType.
	 */
	protected abstract TypeInformation initOutputColType();

	/**
	 * Map the single input column <code>input</code> to a single output.
	 *
	 * @param input the input object
	 * @return the single map result.
	 * @throws Exception
	 */
	protected abstract Object map(Object input) throws Exception;

	@Override
	public TableSchema getOutputSchema() {
		return outputColsHelper.getResultSchema();
	}

	@Override
	public Row map(Row row) throws Exception {
		return this.outputColsHelper.getResultRowSingle(row, map(row.getField(this.colIndex)));
	}
}
