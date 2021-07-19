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

package org.apache.flink.ml.common.model;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * The abstract class for a kind of {@link ModelDataConverter} where the model data can serialize to
 * "Tuple2&jt;Params, Iterable&jt;String&gt;&gt;". Here "Params" is the meta data of the model, and "Iterable&jt;String&gt;" is
 * concrete data of the model.
 */
public abstract class SimpleModelDataConverter<M1, M2> implements ModelDataConverter<M1, M2> {
	/**
	 * Serialize the model data to "Tuple2&jt;Params, Iterable&jt;String&gt;&gt;".
	 *
	 * @param modelData The model data to serialize.
	 * @return A tuple of model meta data stored as {@link Params}, and model concrete data stored as a iterable of strings.
	 */
	public abstract Tuple2<Params, Iterable<String>> serializeModel(M1 modelData);

	/**
	 * Deserialize the model data.
	 *
	 * @param meta The model meta data.
	 * @param data The model concrete data.
	 * @return The deserialized model data.
	 */
	public abstract M2 deserializeModel(Params meta, Iterable<String> data);

	private static final String FIRST_COL_NAME = "model_id";
	private static final String SECOND_COL_NAME = "model_info";
	private static final TypeInformation FIRST_COL_TYPE = Types.LONG();
	private static final TypeInformation SECOND_COL_TYPE = Types.STRING();

	@Override
	public TableSchema getModelSchema() {
		return new TableSchema(
			new String[]{FIRST_COL_NAME, SECOND_COL_NAME},
			new TypeInformation[]{FIRST_COL_TYPE, SECOND_COL_TYPE});
	}

	@Override
	public M2 load(List<Row> rows) {
		Tuple2<Params, Iterable<String>> metaAndData = ModelConverterUtils.extractModelMetaAndData(rows);
		return deserializeModel(metaAndData.f0, metaAndData.f1);
	}

	@Override
	public void save(M1 modelData, Collector<Row> collector) {
		Tuple2<Params, Iterable<String>> model = serializeModel(modelData);
		ModelConverterUtils.appendMetaRow(model.f0, collector, 2);
		ModelConverterUtils.appendDataRows(model.f1, collector, 2);
	}
}
