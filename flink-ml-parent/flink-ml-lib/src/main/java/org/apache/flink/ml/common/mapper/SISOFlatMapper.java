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
import org.apache.flink.ml.params.mode.SISOFlatMapperParams;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * FlatMapper with Single Input column and Single Output column(SISO).
 */
public abstract class SISOFlatMapper extends FlatMapper {
	private final OutputColsHelper outputColsHelper;
	private final int colIndex;
	private ArrayList <Object> buffer = new ArrayList <>();

	/**
	 * Constructor.
	 *
	 * @param dataSchema input data schema.
	 * @param params     input params.
	 */
	public SISOFlatMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		String inputColName = this.params.get(SISOFlatMapperParams.SELECTED_COL);
		this.colIndex = TableUtil.findColIndex(dataSchema.getFieldNames(), inputColName);
		String outputColName = null;
		if (this.params.contains(SISOFlatMapperParams.OUTPUT_COL)) {
			outputColName = params.get(SISOFlatMapperParams.OUTPUT_COL);
		}
		if (null == outputColName) {
			outputColName = inputColName;
		}
		String[] keepColNames = null;
		if (this.params.contains(SISOFlatMapperParams.RESERVED_COLS)) {
			keepColNames = this.params.get(SISOFlatMapperParams.RESERVED_COLS);
		}
		this.outputColsHelper = new OutputColsHelper(dataSchema, outputColName, initOutputColType(), keepColNames);
	}

	/**
	 * Determine the flatmap return type of the {@link SISOFlatMapper#flatMap(Row, Collector)}.
	 * @return the outputColType.
	 */

	protected abstract TypeInformation initOutputColType();

	/**
	 * Flatmap the single input object into a single output.
	 *
	 * @param input  the input data.
	 * @param output the output data.
	 */

	protected abstract void flatMap(Object input, List <Object> output);

	@Override
	public TableSchema getOutputSchema() {
		return outputColsHelper.getResultSchema();
	}

	/**
	 * Wrapper method for the {@link FlatMapper}.
	 * @param row the input row.
	 * @param output
	 */
	@Override
	public void flatMap(Row row, Collector<Row> output) throws Exception {
		this.buffer.clear();
		flatMap(row.getField(this.colIndex), buffer);
		for (Object obj : buffer) {
			output.collect(this.outputColsHelper.getResultRowSingle(row, obj));
		}
	}

}
