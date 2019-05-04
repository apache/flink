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

import org.apache.flink.modelserving.java.model.tensorflow.TensorflowModel;
import org.apache.flink.modelserving.wine.Winerecord;

import org.tensorflow.Tensor;

/**
 * Implementation of tensorflow (optimized) model for testing.
 */
public class WineTensorflowModel extends TensorflowModel<Winerecord.WineRecord, Double> {

	/**
	 * Creates a new tensorflow (optimized) model.
	 *
	 * @param inputStream binary representation of tensorflow (optimized) model.
	 */
	public WineTensorflowModel(byte[] inputStream) throws Throwable {
		super(inputStream);
	}

	/**
	 * Score data.
	 *
	 * @param input object to score.
	 */
	@Override
	public Double score(Winerecord.WineRecord input) {
		// Build input tensor
		Tensor modelInput = WineTensorflowModelFactory.toTensor(input);
		// Serve using tensorflow APIs
		Tensor result = session.runner().feed("dense_1_input", modelInput).fetch("dense_3/Sigmoid").run().get(0);
		// Convert result
		long[] resultshape = result.shape();
		float[][] resulttensor = new float[(int) resultshape[0]][(int) resultshape[1]];
		result.copyTo(resulttensor);
		int sresult = 0;
		float probability = resulttensor[0][0];
		for (int i = 1; i < (int) resultshape[1]; i++){
			if (resulttensor[0][i] > probability){
				sresult = i;
				probability = resulttensor[0][i];
			}
		}
		return (double) sresult;
	}

	/**
	 * Implementation of support class for tensorflow transform.
	 */
	private class Intermediate{
		private int index;
		private float value;

		/**
		 * Create intermediate representation.
		 *
		 * @param i index.
		 * @param v value.
		 */
		public Intermediate(int i, float v){
			index = i;
			value = v;
		}

		/**
		 * Get index.
		 *
		 * @return index.
		 */
		public int getIndex() {
			return index;
		}

		/**
		 * Set index.
		 *
		 * @param index index.
		 */
		public void setIndex(int index) {
			this.index = index;
		}

		/**
		 * Get value.
		 *
		 * @return value.
		 */
		public float getValue() {
			return value;
		}

		/**
		 * Set value.
		 *
		 * @param value value.
		 */
		public void setValue(float value) {
			this.value = value;
		}
	}
}
