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

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * Abstract class for flatMappers with model.
 *
 * <p>The general process of transform the input use machine learning model is:
 * <ul>
 *     <li>1. load the model into memory.</li>
 *     <li>2. process the input using the model.</li>
 * </ul>
 * So, different from the {@link FlatMapper}, this class has a new abstract method
 * named {@link #loadModel(List)} that load the model and transform it to the
 * memory structure.
 *
 * <p>The model is the machine learning model that use the Table as
 * its representation(serialized to Table from the memory
 * or deserialized from Table to memory).
 */
public abstract class FlatModelMapper extends FlatMapper {

	/**
	 * schema of the model with Table type.
	 */
	protected TableSchema modelSchema;

	public FlatModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.modelSchema = modelSchema;
	}

	/**
	 * Load model from the list of Row type data.
	 *
	 * @param modelRows the list of Row type data
	 */
	public abstract void loadModel(List<Row> modelRows);

	/**
	 * Generate new instance of given FlatModelMapper class without model data.
	 * The instance can not deal with real data, but it could be used to get the output result schema.
	 *
	 * @param flatModelMapperClassName Name of the FlatModelMapper class
	 * @param modelScheme              The model scheme represented in Table format.
	 * @param dataSchema               The schema of the input data represented in Table format.
	 * @param params                   The parameters for the instance construction.
	 * @return The object of {@link FlatModelMapper}
	 * @throws Exception if flatModelMapperClass is not the class of {@link FlatModelMapper}
	 */
	public static FlatModelMapper of(
		String flatModelMapperClassName,
		TableSchema modelScheme,
		TableSchema dataSchema,
		Params params) throws Exception {

		return of(flatModelMapperClassName, modelScheme, dataSchema, params, null);
	}

	/**
	 * Generate new instance of given FlatModelMapper class with model data.
	 *
	 * @param flatModelMapperClassName Name of the FlatModelMapper class
	 * @param modelScheme              The model scheme represented in Table format.
	 * @param dataSchema               The schema of the input data represented in Table format.
	 * @param params                   The parameters for the instance construction.
	 * @return The object of {@link FlatModelMapper}
	 * @throws Exception if flatModelMapperClass is not the class of {@link FlatModelMapper}
	 */
	public static FlatModelMapper of(
		String flatModelMapperClassName,
		TableSchema modelScheme,
		TableSchema dataSchema,
		Params params,
		List<Row> modelRows) throws Exception {

		return of(Class.forName(flatModelMapperClassName), modelScheme, dataSchema, params, modelRows);
	}

	/**
	 * Generate new instance of given FlatModelMapper class with model data.
	 *
	 * @param flatModelMapperClass Name of the FlatModelMapper class
	 * @param modelScheme          The model scheme represented in Table format.
	 * @param dataSchema           The schema of the input data represented in Table format.
	 * @param params               The parameters for the instance construction.
	 * @return The object of {@link FlatModelMapper}
	 * @throws Exception if flatModelMapperClass is not the class of {@link FlatModelMapper}
	 */
	public static FlatModelMapper of(
		Class flatModelMapperClass,
		TableSchema modelScheme,
		TableSchema dataSchema,
		Params params,
		List<Row> modelRows) throws Exception {

		if (FlatModelMapper.class.isAssignableFrom(flatModelMapperClass)) {
			FlatModelMapper flatModelMapper = (FlatModelMapper) flatModelMapperClass
				.getConstructor(TableSchema.class, TableSchema.class, Params.class)
				.newInstance(modelScheme, dataSchema, params);

			if (null != modelRows) {
				flatModelMapper.loadModel(modelRows);
			}

			return flatModelMapper;

		} else {

			throw new IllegalArgumentException(
				"FlatModelMapper is not assignable from this class : " + flatModelMapperClass.getCanonicalName());

		}
	}
}
