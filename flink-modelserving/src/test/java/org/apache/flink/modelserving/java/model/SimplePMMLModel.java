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

import org.apache.flink.modelserving.java.model.pmml.PMMLModel;

import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.InputField;

import java.util.List;

/**
 * Implementation of PMML model for testing.
 */
public class SimplePMMLModel extends PMMLModel {

	/**
	 * Creates a new PMML model.
	 *
	 * @param input binary representation of PMML model.
	 */
	public SimplePMMLModel(byte[] input) throws Throwable{

		super(input);
	}

	/**
	 * Score data.
	 *
	 * @param input object to score.
	 */
	@Override
	public Object score(Object input) {
        // Just for test
		return null;
	}

    // Getters for validation
	/**
	 * Get PMML model.
	 * @return PMML.
	 */
	public PMML getPmml() {
		return pmml;
	}

	/**
	 * Get PMML Evaluator.
	 * @return PMML Evaluator.
	 */
	public Evaluator getEvaluator() {
		return evaluator;
	}

	/**
	 * Get PMML model's input field.
	 * @return PMML model input field.
	 */
	public FieldName getTname() {
		return tname;
	}

	/**
	 * Get PMML model output fields.
	 * @return PMML model output fields.
	 */
	public List<InputField> getInputFields() {
		return inputFields;
	}
}
