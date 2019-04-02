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
import org.apache.flink.modelserving.java.model.ModelWithType;
import org.apache.flink.modelserving.java.model.SimpleFactoryResolver;
import org.apache.flink.modelserving.java.server.typeschema.ModelWithTypeSerializer;

import org.junit.BeforeClass;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * Tests for the {@link ModelWithTypeSerializer}.
 */
public class ModelWithTypeSerializerTest extends SerializerTestBase<ModelWithType<Double, Double>> {

	private static String tfmodeloptimized = "model/TF/optimized/optimized_WineQuality.pb";
	private static String tfmodelsaved = "model/TF/saved/";

	private static String dataType = "wine";

	private static byte[] defaultdata = new byte[0];

	@BeforeClass
	public static void oneTimeSetUp() {
        // Set resolver
		DataConverter.setResolver(new SimpleFactoryResolver());
	}

	@Override
	protected TypeSerializer<ModelWithType<Double, Double>> createSerializer() {

		return new ModelWithTypeSerializer<>();
	}

	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Class<ModelWithType<Double, Double>> getTypeClass() {

		return (Class<ModelWithType<Double, Double>>) ((Object) ModelWithType.class);
	}

	@Override
	protected ModelWithType<Double, Double>[] getTestData() {
        // Get TF Optimized model from file
		byte[] model = getModel(tfmodeloptimized);
        // Create model from binary
		Model<Double, Double> tfoptimized = DataConverter.restore(Modeldescriptor.ModelDescriptor.ModelType.TENSORFLOW.getNumber(), model);
        // Get TF bundled model location
		ClassLoader classLoader = getTypeClass().getClassLoader();
		File file = new File(classLoader.getResource(tfmodelsaved).getFile());
		String location = file.getPath();
        // Create model from location
		Model<Double, Double> tfbundled = DataConverter.restore(Modeldescriptor.ModelDescriptor.ModelType.TENSORFLOWSAVED.getNumber(), location.getBytes());
		return new ModelWithType[]{
			new ModelWithType(dataType, Optional.empty()),
			new ModelWithType(dataType, Optional.of(tfoptimized)),
			new ModelWithType(dataType, Optional.of(tfbundled))};
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
