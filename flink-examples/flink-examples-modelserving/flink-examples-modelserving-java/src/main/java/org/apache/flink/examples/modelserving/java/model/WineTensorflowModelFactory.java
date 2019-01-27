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

package org.apache.flink.examples.modelserving.java.model;

import org.apache.flink.modelserving.java.model.Model;
import org.apache.flink.modelserving.java.model.ModelFactory;
import org.apache.flink.modelserving.java.model.ModelToServe;
import org.apache.flink.modelserving.wine.Winerecord;

import org.tensorflow.Tensor;

import java.util.Optional;

/**
 * Implementation of tensorflow (optimized) model factory.
 */
public class WineTensorflowModelFactory implements ModelFactory<Winerecord.WineRecord, Double> {

	private static WineTensorflowModelFactory instance = null;

	/**
	 * Default constructor - protected.
	 */
	private WineTensorflowModelFactory(){}

	/**
	 * Creates a new tensorflow (optimized) model.
	 *
	 * @param descriptor model to serve representation of tensorflow model.
	 * @return model
	 */
	@Override
	public Optional<Model<Winerecord.WineRecord, Double>> create(ModelToServe descriptor) {
		try {
			return Optional.of(new WineTensorflowModel(descriptor.getModelData()));
		}
		catch (Throwable t){
			System.out.println("Exception creating SpecificTensorflowModel from " + descriptor);
			t.printStackTrace();
			return Optional.empty();
		}
	}

	/**
	 * Restore tensorflow (optimised) model from binary.
	 *
	 * @param bytes binary representation of tensorflow model.
	 * @return model
	 */
	@Override
	public Model<Winerecord.WineRecord, Double> restore(byte[] bytes) {
		try {
			return new WineTensorflowModel(bytes);
		}
		catch (Throwable t){
			System.out.println("Exception restoring PMMLModel from ");
			t.printStackTrace();
			return null;
		}
	}

	/**
	 * Convert wine record to tensor.
	 *
	 * @param record wine record.
	 * @return tensor
	 */
	public static Tensor toTensor(Winerecord.WineRecord record){
		float[][] data = {{
			(float) record.getFixedAcidity(),
			(float) record.getVolatileAcidity(),
			(float) record.getCitricAcid(),
			(float) record.getResidualSugar(),
			(float) record.getChlorides(),
			(float) record.getFreeSulfurDioxide(),
			(float) record.getTotalSulfurDioxide(),
			(float) record.getDensity(),
			(float) record.getPH(),
			(float) record.getSulphates(),
			(float) record.getAlcohol()
		}};
		return Tensor.create(data);
	}

	/**
	 * Get model factory instance.
	 *
	 * @return model factory
	 */
	public static ModelFactory<Winerecord.WineRecord, Double> getInstance(){
		if (instance == null) {
			instance = new WineTensorflowModelFactory();
		}
		return instance;
	}
}
