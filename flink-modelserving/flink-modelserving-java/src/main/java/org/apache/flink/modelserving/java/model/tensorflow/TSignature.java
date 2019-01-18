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

package org.apache.flink.modelserving.java.model.tensorflow;

import java.util.Map;

/**
 * Tensorflow bundled Signature definition.
 */
public class TSignature {
	private Map<String, TField> inputs;
	private Map<String, TField> outputs;

	/**
	 * Creates a new Tensorflow Signature.
	 *
	 * @param inputs map of input fields.
	 * @param outputs map of output fields.
	 */
	public TSignature(Map<String, TField> inputs, Map<String, TField> outputs){
		this.inputs = inputs;
		this.outputs = outputs;
	}

	/**
	 * Get signature` inputs.
	 * @return Map of signatures's inputs.
	 */
	public Map<String, TField> getInputs() {

		return inputs;
	}

	/**
	 * Get signature` outputs.
	 * @return Map of signatures's outputs.
	 */
	public Map<String, TField> getOutputs() {

		return outputs;
	}
}
