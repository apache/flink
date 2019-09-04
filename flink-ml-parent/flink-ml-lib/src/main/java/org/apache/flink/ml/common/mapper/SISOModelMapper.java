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

/**
 * ModelMapper with Single Input column and Single Output column(SISO).
 */
public abstract class SISOModelMapper extends ModelMapper {

	private final SISOColsHelper colsHelper;

	public SISOModelMapper(TableSchema modelScheme, TableSchema dataSchema, Params params) {
		super(modelScheme, dataSchema, params);
		this.colsHelper = new SISOColsHelper(dataSchema, initPredResultColType(), params);
	}

	/**
	 * Determine the prediction result type of the {@link SISOModelMapper#predictResult(Object)}.
	 *
	 * @return the outputColType.
	 */
	protected abstract TypeInformation initPredResultColType();

	/**
	 * Predict the single input column <code>input</code> to a single output.
	 *
	 * @param input the input object
	 * @return the single predicted result.
	 * @throws Exception
	 */
	protected abstract Object predictResult(Object input) throws Exception;

	@Override
	public TableSchema getOutputSchema() {
		return colsHelper.getOutputSchema();
	}

	@Override
	public Row map(Row row) throws Exception {
		return this.colsHelper.handleMap(row, this::predictResult);
	}
}
