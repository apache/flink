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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import org.apache.commons.lang3.ArrayUtils;

import java.util.List;

/**
 * The abstract class for a kind of {@link ModelDataConverter} where the model data can serialize to
 * Tuple3&lt;Params, Iterable&lt;String&gt;, Iterable&lt;Row&gt;&gt;. Here "Params" is meta data, "Iterable&lt;String&gt;" is
 * model data, "Iterable&lt;Row&gt;" is auxiliary data.
 */
public abstract class RichModelDataConverter<M1, M2> implements ModelDataConverter<M1, M2> {

	private static final String FIRST_COL_NAME = "model_id";
	private static final String SECOND_COL_NAME = "model_info";
	private static final TypeInformation FIRST_COL_TYPE = Types.LONG;
	private static final TypeInformation SECOND_COL_TYPE = Types.STRING;

	/**
	 * Get the additional column names.
	 */
	protected abstract String[] initAdditionalColNames();

	/**
	 * Get the additional column types.
	 */
	protected abstract TypeInformation[] initAdditionalColTypes();

	/**
	 * Serialize the model data to "Tuple3&lt;Params, Iterable&lt;String&gt;, Iterable&lt;Row&gt;&gt;".
	 *
	 * @param modelData The model data to serialize.
	 * @return The serialization result.
	 */
	public abstract Tuple3<Params, Iterable<String>, Iterable<Row>> serializeModel(M1 modelData);

	/**
	 * Deserialize the model data.
	 *
	 * @param meta         The model meta data.
	 * @param data         The model concrete data.
	 * @param additionData The additional data.
	 * @return The deserialized model data.
	 */
	public abstract M2 deserializeModel(Params meta, Iterable<String> data, Iterable<Row> additionData);

	@Override
	public M2 load(List<Row> rows) {
		Tuple2<Params, Iterable<String>> metaAndData = ModelConverterUtils.extractModelMetaAndData(rows);
		Iterable<Row> additionalData = ModelConverterUtils.extractAuxiliaryData(rows, false);
		return deserializeModel(metaAndData.f0, metaAndData.f1, additionalData);
	}

	@Override
	public void save(M1 modelData, Collector<Row> collector) {
		Tuple3<Params, Iterable<String>, Iterable<Row>> model = serializeModel(modelData);
		ModelConverterUtils.appendMetaRow(model.f0, collector, 2 + initAdditionalColNames().length);
		ModelConverterUtils.appendDataRows(model.f1, collector, 2 + initAdditionalColNames().length);
		ModelConverterUtils.appendAuxiliaryData(model.f2, collector, 2 + initAdditionalColNames().length);
	}

	@Override
	public TableSchema getModelSchema() {
		return new TableSchema(
			ArrayUtils.addAll(new String[]{FIRST_COL_NAME, SECOND_COL_NAME}, initAdditionalColNames()),
			ArrayUtils.addAll(new TypeInformation[]{FIRST_COL_TYPE, SECOND_COL_TYPE}, initAdditionalColTypes())
		);
	}


	/**
	 * Extract select col names from model schema.
	 */
	public static String[] extractSelectedColNames(TableSchema modelSchema) {
		int selectedColNum = modelSchema.getFieldNames().length - 2;
		String[] selectedColNames = new String[selectedColNum];
		for (int i = 0; i < selectedColNum; i++) {
			selectedColNames[i] = modelSchema.getFieldNames()[2 + i];
		}
		return selectedColNames;
	}

	/**
	 * Extract select col types from model schema.
	 */
	public static TypeInformation[] extractSelectedColTypes(TableSchema modelSchema) {
		int selectedColNum = modelSchema.getFieldNames().length - 2;
		TypeInformation[] selectedColTypes = new TypeInformation[selectedColNum];
		for (int i = 0; i < selectedColNum; i++) {
			selectedColTypes[i] = modelSchema.getFieldType(2 + i).get();
		}
		return selectedColTypes;
	}
}
