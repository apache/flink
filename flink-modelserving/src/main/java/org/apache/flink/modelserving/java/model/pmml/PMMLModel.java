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

package org.apache.flink.modelserving.java.model.pmml;

import org.apache.flink.annotation.Public;
import org.apache.flink.model.Modeldescriptor;
import org.apache.flink.modelserving.java.model.Model;

import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.dmg.pmml.Visitor;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.InputField;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.evaluator.TargetField;
import org.jpmml.evaluator.visitors.ExpressionOptimizer;
import org.jpmml.evaluator.visitors.FieldOptimizer;
import org.jpmml.evaluator.visitors.GeneralRegressionModelOptimizer;
import org.jpmml.evaluator.visitors.NaiveBayesModelOptimizer;
import org.jpmml.evaluator.visitors.PredicateOptimizer;
import org.jpmml.evaluator.visitors.RegressionModelOptimizer;
import org.jpmml.model.PMMLUtil;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.List;

/**
 * Base class for PMML model processing.
 */
@Public
public abstract class PMMLModel implements Model {

	// List of optimizerss
	private static List<? extends Visitor> optimizers = Arrays.asList(
		new ExpressionOptimizer(),
		new FieldOptimizer(),
		new PredicateOptimizer(),
		new GeneralRegressionModelOptimizer(),
		new NaiveBayesModelOptimizer(),
		new RegressionModelOptimizer());

	// PMML model
	protected PMML pmml;
	// PMML Evaluator
	protected Evaluator evaluator;
	// Result field name
	protected FieldName tname;
	// Input fields names
	protected List<InputField> inputFields;
	// Byte array
	protected byte[] bytes;

	/**
 	* Creates a new PMML model.
 	*
 	* @param input binary representation of PMML model.
	*/
	public PMMLModel(byte[] input) throws Throwable{
		// Save bytes
		bytes = input;
		// unmarshal PMML
		pmml = PMMLUtil.unmarshal(new ByteArrayInputStream(input));
		// Optimize model
		synchronized (this) {
			for (Visitor optimizer : optimizers) {
				try {
					optimizer.applyTo(pmml);
				} catch (Throwable t){
					// Swallow it
				}
			}
		}

		// Create and verify evaluator
		ModelEvaluatorFactory modelEvaluatorFactory = ModelEvaluatorFactory.newInstance();
		evaluator = modelEvaluatorFactory.newModelEvaluator(pmml);
		evaluator.verify();

		// Get input/target fields
		inputFields = evaluator.getInputFields();
		TargetField target = evaluator.getTargetFields().get(0);
		tname = target.getName();
	}

	/**
 	 * Clean up PMML model.
 	 */
	@Override
	public void cleanup() {
		// Do nothing

	}

	/**
	 * Get bytes representation of PMML model.
	 * @return binary representation of the PMML model.
	 */
	@Override
	public byte[] getBytes() {
		return bytes;
	}

	/**
	 * Get model'a type.
	 * @return PMML model type.
	 */
	@Override
	public long getType() {
		return (long) Modeldescriptor.ModelDescriptor.ModelType.PMML.getNumber();
	}

	/**
	 * Compare 2 PMML models. They are equal if their binary content is same.
	 * @param obj other model.
	 * @return boolean specifying whether models are the same.
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof PMMLModel) {
			return Arrays.equals(((PMMLModel) obj).getBytes(), bytes);
		}
		return false;
	}
}
