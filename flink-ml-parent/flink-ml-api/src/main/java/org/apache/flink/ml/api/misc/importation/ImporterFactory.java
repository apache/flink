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

import org.apache.flink.ml.api.misc.factory.BaseFactory;
import org.apache.flink.ml.api.misc.model.ModelFormat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base factory to create an {@link Importer} to import model data in specific target format as a
 * flink-ml Model.
 *
 * <p>ImporterFactories are loaded by ImporterLoader, and should be registered in the file
 * META-INF/services/org.apache.flink.ml.api.misc.exportation.ImporterFactory.
 *
 * <p>ImporterFactories must have a zero-argument constructor, and have no state, according to
 * the requirement of Java ServiceLoader.
 */
public abstract class ImporterFactory implements BaseFactory {

	/**
	 * Creates and returns an Importer to import model data in source format with the properties.
	 *
	 * @param properties properties that describe the factory configuration
	 * @return importer to import model data in source format
	 */
	public abstract Importer create(Map<String, String> properties);

	/**
	 * Returns the source format of this ImporterFactory.
	 *
	 * @return source format of this ImporterFactory
	 */
	public abstract ModelFormat getSourceFormat();

	@Override
	public final Map<String, String> requiredContext() {
		Map<String, String> identifier = new HashMap<>();
		identifier.put(ImporterLoader.MODEL_FORMAT_KEY, getSourceFormat().name());
		return identifier;
	}

	@Override
	public List<String> requiredProperties() {
		return new ArrayList<>();
	}

	@Override
	public List<String> supportedProperties() {
		return new ArrayList<>();
	}
}
