/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.ml.api.misc.exportation;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.ml.api.core.Model;
import org.apache.flink.ml.api.misc.factory.BaseFactoryLoader;
import org.apache.flink.ml.api.misc.model.ModelFormat;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Loader to get an {@link Exporter} for a specific model format and a model.
 */
@PublicEvolving
public class ExporterLoader extends BaseFactoryLoader<ExporterFactory> {
	public static final String MODEL_FORMAT_KEY = "model_format";
	public static final String MODEL_CLASS_NAME_KEY = "model_class_name";

	private static final ExporterLoader INSTANCE = new ExporterLoader();

	private ExporterLoader() {
	}

	/**
	 * Creates and returns an exporter to export the model in the format with no addition
	 * properties.
	 *
	 * @param format target format to export model in
	 * @param model  model to be exported, used to choose Exporter implementation
	 * @return an exporter to export the model in the target format
	 */
	public static <M extends Model<M>, V> Exporter<M, V> getExporter(ModelFormat format, M model) {
		return getExporter(format, model, new HashMap<>());
	}

	/**
	 * Creates and returns an exporter to export the model in the format with the properties.
	 *
	 * @param format     target format to export model in
	 * @param model      model to be exported, used to choose Exporter implementation
	 * @param properties properties required by the exporter
	 * @return an exporter to export the model in the target format
	 */
	public static <M extends Model<M>, V> Exporter<M, V> getExporter(
		ModelFormat format,
		M model,
		Map<String, String> properties) {

		return getExporter(format, model, properties, null);
	}

	/**
	 * Creates and returns an exporter to export the model in the format with the properties, the
	 * exporter is loaded by the specific ClassLoader if the loader is not null.
	 *
	 * @param format     target format to export model in
	 * @param model      model to be exported, used to choose Exporter implementation
	 * @param properties properties required by the exporter
	 * @param loader     classloader for exporter loading
	 * @return an exporter to export the model in the target format
	 */
	public static <M extends Model<M>, V> Exporter<M, V> getExporter(
		ModelFormat format,
		M model,
		Map<String, String> properties,
		@Nullable ClassLoader loader) {

		Map<String, String> fullProperties = new HashMap<>();
		fullProperties.put(MODEL_FORMAT_KEY, format.name());
		fullProperties.put(MODEL_CLASS_NAME_KEY, model.getClass().getName());
		fullProperties.putAll(properties);

		return INSTANCE.find(ExporterFactory.class, fullProperties, loader)
			.create(model, properties);
	}

	@Override
	protected Class<ExporterFactory> getFactoryClass() {
		return ExporterFactory.class;
	}

	@Override
	protected boolean matchCustomizedRequirements(
		ExporterFactory factory,
		Map<String, String> properties) {

		return factory.supportedModelClassNames().contains(properties.get(MODEL_CLASS_NAME_KEY));
	}
}
