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

package org.apache.flink.modelserving.java.model;

import org.apache.flink.model.Modeldescriptor;
import org.apache.flink.modelserving.java.model.tensorflow.TField;
import org.apache.flink.modelserving.java.model.tensorflow.TSignature;

import com.google.protobuf.ByteString;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link DataConverter}.
 */
public class DataConverterTest {

	private static byte[] baddata = new byte[0];

	private static String tfmodeloptimized = "model/TF/optimized/optimized_WineQuality.pb";
	private static String tfmodelsaved = "model/TF/saved/";
	private static String name = "test";
	private static String description = "test";
	private static String dataType = "simple";

	private static String bundleTag = "serve";
	private static String bundleSignature = "serving_default";
	private static String bundleInputs = "inputs";
	private static TField input = new TField("image_tensor", null, Arrays.asList(-1, -1, -1, 3));
	private static List<String> bundleoutputs = Arrays.asList("detection_classes", "detection_boxes", "num_detections", "detection_scores");
	private static List<TField> output = Arrays.asList(
		new TField("detection_classes", null, Arrays.asList(-1, 100)),
		new TField("detection_boxes", null, Arrays.asList(-1, 100, 4)),
		new TField("num_detections", null, Arrays.asList(-1)),
		new TField("detection_scores", null, Arrays.asList(-1, 100))
	);

	@BeforeClass
	public static void oneTimeSetUp() {
        // Set resolver
		DataConverter.setResolver(new SimpleFactoryResolver());
	}

	@Test
	public void testTFOptimized() {
        // Get TF model from File
		byte[] model = getModel(tfmodeloptimized);
        // Build input record
		byte[] record = getbinaryContent(model, null, Modeldescriptor.ModelDescriptor.ModelType.TENSORFLOW);
        // Convert input record
		Optional<ModelToServe> result = DataConverter.convertModel(record);
        // validate it
		validateModelToServe(result, model, null, Modeldescriptor.ModelDescriptor.ModelType.TENSORFLOW);
        // Build TF model
		Optional<Model<Double, Double>> tf = DataConverter.toModel(result.get());
        // Validate
		assertTrue("TF Model created", tf.isPresent());
		valdateTFModel(tf.get());
        // Simply copy the model
		Model copyDirect = DataConverter.copy(tf.get());
        // Validate
		assertEquals("Copy equal to source", tf.get(), copyDirect);
        // Create model from binary
		Model<Double, Double> direct = DataConverter.restore(Modeldescriptor.ModelDescriptor.ModelType.TENSORFLOW.getNumber(), model);
        // Validate it
		valdateTFModel(direct);
	}

	@Test
	public void testTFOptimizedBadData() {
        // Get TF model from File
		byte[] model = new byte[0];
        // Build input record
		byte[] record = getbinaryContent(model, null, Modeldescriptor.ModelDescriptor.ModelType.TENSORFLOW);
        // Convert input record
		Optional<ModelToServe> result = DataConverter.convertModel(record);
        // validate it
		validateModelToServe(result, model, null, Modeldescriptor.ModelDescriptor.ModelType.TENSORFLOW);
        // Build TF model
		Optional<Model<Double, Double>> tf = DataConverter.toModel(result.get());
        // Validate
		assertFalse("TF Model created", tf.isPresent());
	}

	@Test
	public void testTFBundled() {
        // Get TF model from File
		ClassLoader classLoader = getClass().getClassLoader();
		File file = new File(classLoader.getResource(tfmodelsaved).getFile());
		String model = file.getPath();
        // Build input record
		byte[] record = getbinaryContent(null, model, Modeldescriptor.ModelDescriptor.ModelType.TENSORFLOWSAVED);
        // Convert input record
		Optional<ModelToServe> result = DataConverter.convertModel(record);
        // validate it
		validateModelToServe(result, null, model, Modeldescriptor.ModelDescriptor.ModelType.TENSORFLOWSAVED);
        // Build TF model
		Optional<Model<Double, Double>> tf = DataConverter.toModel(result.get());
        // Validate
		assertTrue("TF Model created", tf.isPresent());
		valdateTFBundleModel(tf.get());
        // Simply copy the model
		Model copyDirect = DataConverter.copy(tf.get());
        // Validate
		assertEquals("Copy equal to source", tf.get(), copyDirect);
		valdateTFBundleModel(copyDirect);
        // Create model from binary
		Model<Double, Double> direct = DataConverter.restore(Modeldescriptor.ModelDescriptor.ModelType.TENSORFLOWSAVED.getNumber(), model.getBytes());
        // Validate it
		valdateTFBundleModel(direct);
	}

	@Test
	public void testTFBundledBadData() {
		String model = new String();
        // Build input record
		byte[] record = getbinaryContent(null, model, Modeldescriptor.ModelDescriptor.ModelType.TENSORFLOWSAVED);
        // Convert input record
		Optional<ModelToServe> result = DataConverter.convertModel(record);
        // validate it
		validateModelToServe(result, null, model, Modeldescriptor.ModelDescriptor.ModelType.TENSORFLOWSAVED);
        // Build TF model
		Optional<Model<Double, Double>> tf = DataConverter.toModel(result.get());
        // Validate
		assertFalse("TF Model is not created", tf.isPresent());
	}

	private void valdateTFModel(Model<Double, Double> tf) {
		assertTrue(tf instanceof SimpleTensorflowModel);
		SimpleTensorflowModel tfModel = (SimpleTensorflowModel) tf;
		assertNotEquals("Graph is created", null, tfModel.getGrapth());
		assertNotEquals("Session is created", null, tfModel.getSession());
	}

	private void valdateTFBundleModel(Model<Double, Double> tf) {
		assertTrue(tf instanceof SimpleTensorflowBundleModel);
		SimpleTensorflowBundleModel tfModel = (SimpleTensorflowBundleModel) tf;
		assertNotEquals("Graph is created", null, tfModel.getGraph());
		assertNotEquals("Session is created", null, tfModel.getSession());
		assertEquals(1, tfModel.getTags().size());
		assertEquals("Tag is correct", bundleTag, tfModel.getTags().get(0));
		assertEquals("Number of signatures is correct", 1, tfModel.getSignatures().size());
		Map.Entry<String, TSignature> sigEntry = tfModel.getSignatures().entrySet().iterator().next();
		assertEquals("Signature name is correct", bundleSignature, sigEntry.getKey());
		TSignature sign = sigEntry.getValue();
		assertEquals("Number of inputs is correct", 1, sign.getInputs().size());
		Map.Entry<String, TField> inputEntry = sign.getInputs().entrySet().iterator().next();
		assertEquals("Input name is correct", bundleInputs, inputEntry.getKey());
		assertEquals("Input name is correct", input.getName(), inputEntry.getValue().getName());
		assertArrayEquals("Input shape is correct", input.getShape().toArray(new Integer[0]), inputEntry.getValue().getShape().toArray(new Integer[0]));
		assertEquals("Number of outputs is correct", 4, sign.getOutputs().size());
		Iterator<TField> outputIterator = output.iterator();
		for (String outputName : bundleoutputs) {
			TField current = sign.getOutputs().get(outputName);
			assertNotEquals("Output name is correct", null, current);
			TField field = outputIterator.next();
			assertEquals("Output name is correct", field.getName(), current.getName());
			assertArrayEquals("Output shape is correct", field.getShape().toArray(new Integer[0]), current.getShape().toArray(new Integer[0]));
		}
	}

	private void validateModelToServe(Optional<ModelToServe> modelToServe, byte[] model, String location, Modeldescriptor.ModelDescriptor.ModelType type){
		assertTrue("Model is created", modelToServe.isPresent());
		assertEquals("Model type is correct", type, modelToServe.get().getModelType());
		assertEquals("Data type is correct", dataType, modelToServe.get().getDataType());
		assertEquals("Model name is correct", name, modelToServe.get().getName());
		assertEquals("Model description is correct", description, modelToServe.get().getDescription());
		if (model != null){
			assertArrayEquals("Model data is correct", model, modelToServe.get().getModelData());
		}
		else {
			assertEquals("Model location is correct", location, modelToServe.get().getModelDataLocation());
		}
	}

	private byte[] getModel(String fileName) {
		ClassLoader classLoader = getClass().getClassLoader();
		File file = new File(classLoader.getResource(fileName).getFile());
		try {
			return Files.readAllBytes(Paths.get(file.getPath()));
		} catch (Throwable t){
			return baddata;
		}
	}

	private byte[] getbinaryContent(byte[] pByteArray, String location, Modeldescriptor.ModelDescriptor.ModelType type) {
		Modeldescriptor.ModelDescriptor.Builder builder = Modeldescriptor.ModelDescriptor.newBuilder();
		builder.setModeltype(type);
		builder.setDataType(dataType);
		if (location != null){
			builder.setLocation(location);
		} else {
			builder.setData(ByteString.copyFrom(pByteArray));
		}
		builder.setName(name);
		builder.setDescription(description);
		Modeldescriptor.ModelDescriptor record = builder.build();
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try {
			record.writeTo(bos);
			return bos.toByteArray();
		} catch (Throwable t){
			return baddata;
		}
	}
}
