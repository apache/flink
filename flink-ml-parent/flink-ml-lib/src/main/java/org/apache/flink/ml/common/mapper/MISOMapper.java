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
import org.apache.flink.ml.params.mapper.MISOMapperParams;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

/**
 * Mapper with Multi-Input columns and Single Output column(MISO).
 */
public abstract class MISOMapper extends Mapper {
	private final OutputColsHelper outputColsHelper;
	private final int[] colIndices;

	/**
	 * Constructor.
	 *
	 * @param dataSchema input table schema.
	 * @param params     input parameters.
	 */
	public MISOMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		String[] inputColNames = this.params.get(MISOMapperParams.SELECTED_COLS);
		this.colIndices = TableUtil.findColIndices(dataSchema.getFieldNames(), inputColNames);
		String outputColName = params.get(MISOMapperParams.OUTPUT_COL);
		String[] keepColNames = null;
		if (this.params.contains(MISOMapperParams.RESERVED_COLS)) {
			keepColNames = this.params.get(MISOMapperParams.RESERVED_COLS);
		}
		this.outputColsHelper = new OutputColsHelper(dataSchema, outputColName, initOutputColType(), keepColNames);
	}

	/**
	 * Determine the return type of the {@link MISOMapper#map(Object[])}.
	 *
	 * @return the output column type.
	 */
	protected abstract TypeInformation initOutputColType();

	/**
	 * Map input objects to single object.
	 *
	 * @param input input objects.
	 * @return      output object.
	 */
	protected abstract Object map(Object[] input) throws Exception;

	@Override
	public TableSchema getOutputSchema() {
		return outputColsHelper.getResultSchema();
	}

	@Override
	public Row map(Row row) throws Exception {
		Object[] input = new Object[this.colIndices.length];
		for (int i = 0; i < this.colIndices.length; i++) {
			input[i] = row.getField(this.colIndices[i]);
		}
		return this.outputColsHelper.getResultRow(row, Row.of(map(input)));
	}
}
