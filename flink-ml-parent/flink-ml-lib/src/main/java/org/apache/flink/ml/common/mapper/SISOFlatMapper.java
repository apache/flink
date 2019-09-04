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
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * FlatMapper with Single Input column and Single Output column(SISO).
 */
public abstract class SISOFlatMapper extends FlatMapper {

	private final SISOColsHelper colHelper;

	/**
	 * Constructor.
	 *
	 * @param dataSchema input data schema.
	 * @param params     input params.
	 */
	public SISOFlatMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.colHelper = new SISOColsHelper(dataSchema, initOutputColType(), params);
	}

	/**
	 * Determine the flatmap return type of the {@link SISOFlatMapper#flatMap(Row, Collector)}.
	 *
	 * @return the outputColType.
	 */

	protected abstract TypeInformation initOutputColType();

	/**
	 * Flatmap the single input object into a single output.
	 *
	 * @param input  the input data.
	 * @param output the output data collector.
	 */

	protected abstract void flatMap(Object input, Collector<Object> output);

	@Override
	public TableSchema getOutputSchema() {
		return colHelper.getOutputSchema();
	}

	/**
	 * Wrapper method for the {@link FlatMapper}.
	 *
	 * @param row    the input row.
	 * @param output
	 */
	@Override
	public void flatMap(Row row, Collector<Row> output) throws Exception {
		this.colHelper.handleFlatMap(row, output, this::flatMap);
	}

}
