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

package org.apache.flink.modelserving.java.server;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.model.Modeldescriptor;
import org.apache.flink.modelserving.java.model.DataConverter;
import org.apache.flink.modelserving.java.model.Model;
import org.apache.flink.modelserving.java.model.SimpleFactoryResolver;
import org.apache.flink.modelserving.java.server.typeschema.ModelTypeSerializer;

import org.junit.BeforeClass;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Tests for the {@link ModelTypeSerializer}.
 */
public class ModelTypeSerializerTest extends SerializerTestBase<Model<Double, Double>>{

	private static String tfmodeloptimized = "model/TF/optimized/optimized_WineQuality.pb";
	private static String tfmodelsaved = "model/TF/saved/";
	private static String pmmlmodel = "model/PMML/winequalityDecisionTreeClassification.pmml";

	private static final byte[] defaultdata = new byte[0];

	@BeforeClass
	public static void oneTimeSetUp() {
        // Set resolver
		DataConverter.setResolver(new SimpleFactoryResolver());
	}

	@Override
	protected TypeSerializer<Model<Double, Double>> createSerializer() {
		return new ModelTypeSerializer<>();
	}

	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Class<Model<Double, Double>> getTypeClass() {
		return (Class<Model<Double, Double>>) ((Object) Model.class);
	}

	@Override
	protected Model<Double, Double>[] getTestData() {
        // Get PMML model from File
		byte[] model = getModel(pmmlmodel);
        // Create model from binary
		Model<Double, Double> pmml = DataConverter.restore(Modeldescriptor.ModelDescriptor.ModelType.PMML.getNumber(), model);
        // Get TF Optimized model from file
		model = getModel(tfmodeloptimized);
        // Create model from binary
		Model tfoptimized = DataConverter.restore(Modeldescriptor.ModelDescriptor.ModelType.TENSORFLOW.getNumber(), model);
        // Get TF bundled model location
		ClassLoader classLoader = getTypeClass().getClassLoader();
		File file = new File(classLoader.getResource(tfmodelsaved).getFile());
		String location = file.getPath();
        // Create model from location
		Model<Double, Double> tfbundled = DataConverter.restore(Modeldescriptor.ModelDescriptor.ModelType.TENSORFLOWSAVED.getNumber(), location.getBytes());
		return new Model[]{null, pmml, tfoptimized, tfbundled};
	}

	private byte[] getModel(String fileName) {
		ClassLoader classLoader = getTypeClass().getClassLoader();
		try {
			File file = new File(classLoader.getResource(fileName).getFile());
			return Files.readAllBytes(Paths.get(file.getPath()));
		} catch (Throwable t) {
			return defaultdata;
		}
	}
}
