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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.utils.OutputColsHelper;
import org.apache.flink.ml.params.mapper.RichModelMapperParams;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

/**
 * Abstract class for mappers with rich model.
 *
 * <p>The RichModel is used to the classification, the regression or the clustering.
 * The output of the model mapper using RichModel as its model contains three part:
 * <ul>
 *     <li>The reserved columns from input</li>
 *     <li>The prediction result column</li>
 *     <li>The prediction detail column</li>
 * </ul>
 */
public abstract class RichModelMapper extends ModelMapper {

	/**
	 * The output column helper which control the output format.
	 *
	 * @see OutputColsHelper
	 */
	private final OutputColsHelper outputColsHelper;

	/**
	 * The condition that the mapper output the prediction detail or not.
	 */
	private final boolean isPredDetail;

	public RichModelMapper(TableSchema modelScheme, TableSchema dataSchema, Params params) {
		super(modelScheme, dataSchema, params);
		String[] keepColNames = this.params.get(RichModelMapperParams.RESERVED_COLS);
		String predResultColName = this.params.get(RichModelMapperParams.PREDICTION_COL);
		TypeInformation predResultColType = initPredResultColType();
		isPredDetail = params.contains(RichModelMapperParams.PREDICTION_DETAIL_COL);
		if (isPredDetail) {
			String predDetailColName = params.get(RichModelMapperParams.PREDICTION_DETAIL_COL);
			this.outputColsHelper = new OutputColsHelper(dataSchema,
				new String[]{predResultColName, predDetailColName},
				new TypeInformation[]{predResultColType, Types.STRING}, keepColNames);
		} else {
			this.outputColsHelper = new OutputColsHelper(dataSchema, predResultColName, predResultColType,
				keepColNames);
		}
	}

	/**
	 * Initial the prediction result column type.
	 *
	 * <p>The subclass can override this method to initial the {@link OutputColsHelper}
	 *
	 * @return the type of the prediction result column
	 */
	protected TypeInformation initPredResultColType() {
		return super.modelSchema.getFieldTypes()[2];
	}

	@Override
	public TableSchema getOutputSchema() {
		return outputColsHelper.getResultSchema();
	}

	/**
	 * Calculate the prediction result.
	 *
	 * @param row the input
	 * @return the prediction result.
	 */
	protected abstract Object predictResult(Row row) throws Exception;

	/**
	 * Calculate the prediction result ant the prediction detail.
	 *
	 * @param row the input
	 * @return The prediction result and the the prediction detail.
	 */
	protected abstract Tuple2<Object, String> predictResultDetail(Row row) throws Exception;

	@Override
	public Row map(Row row) throws Exception {
		if (isPredDetail) {
			Tuple2<Object, String> t2 = predictResultDetail(row);
			return this.outputColsHelper.getResultRow(row, Row.of(t2.f0, t2.f1));
		} else {
			return this.outputColsHelper.getResultRow(row, Row.of(predictResult(row)));
		}
	}
}
