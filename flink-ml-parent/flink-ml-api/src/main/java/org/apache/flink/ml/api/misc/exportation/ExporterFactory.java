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

import org.apache.flink.ml.api.core.Model;
import org.apache.flink.ml.api.misc.factory.BaseFactory;
import org.apache.flink.ml.api.misc.model.ModelFormat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base factory to create an {@link Exporter} to export models into model data in specific target
 * format.
 *
 * <p>ExporterFactories are loaded by ExporterLoader, and should be registered in the file
 * META-INF/services/org.apache.flink.ml.api.misc.exportation.ExporterFactory.
 *
 * <p>ExporterFactories must have a zero-argument constructor, and have no state, according to
 * the requirement of Java ServiceLoader.
 */
public abstract class ExporterFactory implements BaseFactory {

	/**
	 * Creates and returns an Exporter to export the model to target format with the properties.
	 *
	 * @param model      model to export
	 * @param properties properties that describe the factory configuration
	 * @return exporter to export the model to target format
	 */
	public abstract Exporter create(Model model, Map<String, String> properties);

	/**
	 * Returns the target format of this ExporterFactory.
	 *
	 * @return target format of this ExporterFactory
	 */
	public abstract ModelFormat getTargetFormat();

	/**
	 * Returns a list of model class names that this ExporterFactory supports. The values in the
	 * list should be in the same format returned by {@link java.lang.Class#getName}.
	 *
	 * @return a list of model class names that this ExporterFactory supports
	 */
	public abstract List<String> supportedModelClassNames();

	@Override
	public final Map<String, String> requiredContext() {
		Map<String, String> identifier = new HashMap<>();
		identifier.put(ExporterLoader.MODEL_FORMAT_KEY, getTargetFormat().name());
		return identifier;
	}

	@Override
	public List<String> requiredProperties() {
		return new ArrayList<>();
	}

	@Override
	public List<String> supportedProperties() {
		List<String> supportedProperties = new ArrayList<>();
		supportedProperties.add(ExporterLoader.MODEL_FORMAT_KEY);
		supportedProperties.add(ExporterLoader.MODEL_CLASS_NAME_KEY);
		return supportedProperties;
	}
}
