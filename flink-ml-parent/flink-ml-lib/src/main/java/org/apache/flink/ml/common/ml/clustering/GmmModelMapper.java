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

package org.apache.flink.ml.common.ml.clustering;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.linalg.Vector;
import org.apache.flink.ml.common.mapper.RichModelMapper;
import org.apache.flink.ml.common.statistics.basicstatistic.MultivariateGaussian;
import org.apache.flink.ml.common.utils.JsonConverter;
import org.apache.flink.ml.common.utils.TableUtil;
import org.apache.flink.ml.params.ml.clustering.GmmPredictParams;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Model mapper of Gaussian Mixture Model.
 */
public class GmmModelMapper extends RichModelMapper {

	private int vectorColIdx;
	private GmmModelData modelData;
	private MultivariateGaussian[] multivariateGaussians;
	private double[] prob;

	public GmmModelMapper(TableSchema modelScheme, TableSchema dataSchema, Params params) {
		super(modelScheme, dataSchema, params);
		String vectorColName = this.params.get(GmmPredictParams.VECTOR_COL);
		vectorColIdx = TableUtil.findColIndex(dataSchema.getFieldNames(), vectorColName);
		if (vectorColIdx < 0) {
			throw new RuntimeException("Can't find vectorCol: " + vectorColName);
		}
	}

	@Override
	protected Object predictResult(Row row) throws Exception {
		return predictResultDetail(row).f0;
	}

	@Override
	protected Tuple2<Object, String> predictResultDetail(Row row) throws Exception {
		Vector sample = (Vector) row.getField(vectorColIdx);

		int k = modelData.k;
		double probSum = 0.;
		for (int i = 0; i < k; i++) {
			double density = this.multivariateGaussians[i].pdf(sample);
			double p = modelData.data.get(i).weight * density;
			prob[i] = p;
			probSum += p;
		}
		for (int i = 0; i < k; i++) {
			prob[i] /= probSum;
		}

		int maxIndex = 0;
		double maxProb = prob[0];

		for (int i = 1; i < k; i++) {
			if (prob[i] > maxProb) {
				maxProb = prob[i];
				maxIndex = i;
			}
		}

		Map<Integer, Double> detail = new HashMap<>();
		for (int i = 0; i < k; i++) {
			detail.put(i, prob[i]);
		}

		return Tuple2.of((long) maxIndex, JsonConverter.toJson(detail));
	}

	@Override
	protected TypeInformation initPredResultColType() {
		return Types.LONG;
	}

	@Override
	public void loadModel(List<Row> modelRows) {
		this.modelData = new GmmModelDataConverter().load(modelRows);
		this.multivariateGaussians = new MultivariateGaussian[this.modelData.k];
		for (int i = 0; i < this.modelData.k; i++) {
			this.multivariateGaussians[i] = new MultivariateGaussian(modelData.data.get(i).mean,
				GmmModelData.expandCovarianceMatrix(modelData.data.get(i).cov, modelData.dim));
		}
		this.prob = new double[this.modelData.k];
	}
}
