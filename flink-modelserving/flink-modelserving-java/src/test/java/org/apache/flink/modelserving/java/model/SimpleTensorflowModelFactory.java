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

import java.util.Optional;

/**
 * Implementation of tensorflow (optimized) model factory.
 */
public class SimpleTensorflowModelFactory implements ModelFactory<Double, Double> {

	private static SimpleTensorflowModelFactory instance = null;

	/**
	 * Default constructor - protected.
	 */
	private SimpleTensorflowModelFactory(){}

	/**
	 * Creates a new tensorflow (optimized) model.
	 *
	 * @param descriptor model to serve representation of tensorflow model.
	 * @return model
	 */
	@Override
	public Optional<Model<Double, Double>> create(ModelToServe descriptor) {
		try {
			return Optional.of(new SimpleTensorflowModel(descriptor.getModelData()));
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
	public Model restore(byte[] bytes) {
		try {
			return new SimpleTensorflowModel(bytes);
		}
		catch (Throwable t){
			System.out.println("Exception restoring PMMLModel from ");
			t.printStackTrace();
			return null;
		}
	}

	/**
	 * Get model factory instance.
	 *
	 * @return model factory
	 */
	public static ModelFactory getInstance(){
		if (instance == null) {
			instance = new SimpleTensorflowModelFactory();
		}
		return instance;
	}
}
