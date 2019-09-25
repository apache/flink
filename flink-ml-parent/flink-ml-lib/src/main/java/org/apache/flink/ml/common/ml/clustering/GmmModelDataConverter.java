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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.model.SimpleModelDataConverter;
import org.apache.flink.ml.common.utils.JsonConverter;
import org.apache.flink.ml.params.ml.clustering.GmmTrainParams;

import java.util.ArrayList;
import java.util.List;

/**
 * Model data converter for Gaussian Mixture model.
 */
public class GmmModelDataConverter extends SimpleModelDataConverter<GmmModelData, GmmModelData> {
	private static final ParamInfo<Integer> NUM_FEATURES = ParamInfoFactory
		.createParamInfo("numFeatures", Integer.class)
		.setRequired()
		.build();

	@Override
	public Tuple2<Params, Iterable<String>> serializeModel(GmmModelData modelData) {
		List<String> data = new ArrayList<>();
		for (GmmModelData.ClusterSummary clusterSummary : modelData.data) {
			data.add(JsonConverter.toJson(clusterSummary));
		}
		Params meta = new Params()
			.set(NUM_FEATURES, modelData.dim)
			.set(GmmTrainParams.K, modelData.k)
			.set(GmmTrainParams.VECTOR_COL, modelData.vectorCol);

		return Tuple2.of(meta, data);
	}

	@Override
	public GmmModelData deserializeModel(Params meta, Iterable<String> data) {
		GmmModelData modelData = new GmmModelData();
		modelData.k = meta.get(GmmTrainParams.K);
		modelData.vectorCol = meta.get(GmmTrainParams.VECTOR_COL);
		modelData.dim = meta.get(NUM_FEATURES);
		modelData.data = new ArrayList<>(modelData.k);
		for (String row : data) {
			modelData.data.add(JsonConverter.fromJson(row, GmmModelData.ClusterSummary.class));
		}

		return modelData;
	}
}
