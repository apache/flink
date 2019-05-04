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

import org.apache.flink.modelserving.java.model.tensorflow.TField;
import org.apache.flink.modelserving.java.model.tensorflow.TSignature;
import org.apache.flink.modelserving.java.model.tensorflow.TensorflowBundleModel;
import org.apache.flink.modelserving.wine.Winerecord;

import org.tensorflow.Tensor;

/**
 * Implementation of PMML model for testing.
 */
public class WineTensorflowBundledModel extends TensorflowBundleModel<Winerecord.WineRecord, Double> {

	/**
	 * Creates a new tensorflow (bundled) model.
	 *
	 * @param input binary representation of PMML model.
	 */
	public WineTensorflowBundledModel(byte[] input) throws Throwable{
		super(input);
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
		TSignature signature = signatures.entrySet().iterator().next().getValue();
		TField tinput = signature.getInputs().entrySet().iterator().next().getValue();
		TField toutput = signature.getOutputs().entrySet().iterator().next().getValue();
		Tensor result = session.runner().feed(tinput.getName(), modelInput).fetch(toutput.getName()).run().get(0);
		// process result
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
}
