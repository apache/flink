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

package org.apache.flink.ml.api.misc.importation;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.ml.api.core.Model;
import org.apache.flink.ml.api.misc.factory.BaseFactoryLoader;
import org.apache.flink.ml.api.misc.model.ModelFormat;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Loader to get an {@link Importer} for a specific model format.
 */
@PublicEvolving
public class ImporterLoader extends BaseFactoryLoader<ImporterFactory> {
	public static final String MODEL_FORMAT_KEY = "model_format";

	private static final ImporterLoader INSTANCE = new ImporterLoader();

	private ImporterLoader() {
	}

	/**
	 * Creates and returns an importer to import model data in the source format with no addition
	 * properties.
	 *
	 * @param format source format to import model
	 * @return an import to import model data in the source format
	 */
	public static <M extends Model<M>, V> Importer<M, V> getImporter(ModelFormat format) {
		return getImporter(format, new HashMap<>());
	}

	/**
	 * Creates and returns an importer to import model data in the source format with the
	 * properties.
	 *
	 * @param format     source format to import model
	 * @param properties properties required by the importer
	 * @return an import to import model data in the source format
	 */
	public static <M extends Model<M>, V> Importer<M, V> getImporter(
		ModelFormat format,
		Map<String, String> properties) {
		return getImporter(format, properties, null);
	}

	/**
	 * Creates and returns an importer to import model data in the source format with the
	 * properties, the importer is loaded by the specific ClassLoader if the loader is not null.
	 *
	 * @param format     source format to import model
	 * @param properties properties required by the importer
	 * @param loader     classloader for importer loading
	 * @return an import to import model data in the source format
	 */
	public static <M extends Model<M>, V> Importer<M, V> getImporter(
		ModelFormat format,
		Map<String, String> properties,
		@Nullable ClassLoader loader) {

		Map<String, String> fullProperties = new HashMap<>();
		fullProperties.put(MODEL_FORMAT_KEY, format.name());
		fullProperties.putAll(properties);

		return INSTANCE.find(ImporterFactory.class, fullProperties, loader).create(properties);
	}

	@Override
	protected Class<ImporterFactory> getFactoryClass() {
		return ImporterFactory.class;
	}
}
