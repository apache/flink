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
import org.apache.flink.util.Preconditions;

import java.util.List;

import static org.apache.flink.ml.common.model.ModelConverterUtils.appendAuxiliaryData;
import static org.apache.flink.ml.common.model.ModelConverterUtils.appendDataRows;
import static org.apache.flink.ml.common.model.ModelConverterUtils.appendMetaRow;
import static org.apache.flink.ml.common.model.ModelConverterUtils.extractAuxiliaryData;
import static org.apache.flink.ml.common.model.ModelConverterUtils.extractModelMetaAndData;

/**
 * The abstract class for a kind of {@link ModelDataConverter} where the model data can serialize to
 * Tuple3&lt;Params, Iterable&lt;String&gt;, Iterable&lt;Object&gt;&gt;. Here "Params" is meta data, "Iterable&lt;String&gt;"
 * is model data, "Iterable&lt;Object&gt;" is distinct labels.
 */
public abstract class LabeledModelDataConverter<M1, M2> implements ModelDataConverter<M1, M2> {

	private static final String FIRST_COL_NAME = "model_id";
	private static final String SECOND_COL_NAME = "model_info";
	private static final String LABEL_COL_NAME = "label_value";
	private static final TypeInformation FIRST_COL_TYPE = Types.LONG;
	private static final TypeInformation SECOND_COL_TYPE = Types.STRING;

	/**
	 * Data type of labels.
	 */
	protected TypeInformation labelType;

	public LabeledModelDataConverter() {
	}

	public LabeledModelDataConverter(TypeInformation labelType) {
		this.labelType = labelType;
	}

	/**
	 * Serialize the model to "Tuple3&lt;Params, Iterable&lt;String&gt;, Iterable&lt;Object&gt;&gt;".
	 *
	 * @param modelData The model data to serialize.
	 * @return The serialization result.
	 */
	protected abstract Tuple3<Params, Iterable<String>, Iterable<Object>> serializeModel(M1 modelData);

	/**
	 * Deserialize the model from "Params meta", "List&lt;String&gt; data" and "List&lt;Object&gt;&gt;".
	 *
	 * @param meta           The model meta data.
	 * @param data           The model concrete data.
	 * @param distinctLabels Distinct label values of training data.
	 * @return The deserialized model data.
	 */
	protected abstract M2 deserializeModel(Params meta, Iterable<String> data, Iterable<Object> distinctLabels);

	/**
	 * A utility function to extract label type from model schema.
	 */
	public static TypeInformation extractLabelType(TableSchema modelSchema) {
		return modelSchema.getFieldTypes()[2];
	}

	@Override
	public M2 load(List<Row> rows) {
		Tuple2<Params, Iterable<String>> metaAndData = extractModelMetaAndData(rows);
		Iterable<Object> labels = extractAuxiliaryData(rows, true);
		return deserializeModel(metaAndData.f0, metaAndData.f1, labels);

	}

	@Override
	public void save(M1 modelData, Collector<Row> collector) {
		Tuple3<Params, Iterable<String>, Iterable<Object>> model = serializeModel(modelData);
		appendMetaRow(model.f0, collector, 3);
		appendDataRows(model.f1, collector, 3);
		appendAuxiliaryData(model.f2, collector, 3);
	}

	@Override
	public TableSchema getModelSchema() {
		Preconditions.checkArgument(labelType != null, "label type is null.");
		return new TableSchema(
			new String[]{FIRST_COL_NAME, SECOND_COL_NAME, LABEL_COL_NAME},
			new TypeInformation[]{FIRST_COL_TYPE, SECOND_COL_TYPE, labelType}
		);
	}
}
