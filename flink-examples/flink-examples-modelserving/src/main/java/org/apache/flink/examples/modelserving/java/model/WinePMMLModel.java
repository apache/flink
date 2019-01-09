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

import org.apache.flink.modelserving.java.model.pmml.PMMLModel;
import org.apache.flink.modelserving.wine.Winerecord;

import com.google.protobuf.Descriptors;
import org.dmg.pmml.FieldName;
import org.jpmml.evaluator.Computable;
import org.jpmml.evaluator.FieldValue;
import org.jpmml.evaluator.InputField;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Implementation of PMML model for testing.
 */
public class WinePMMLModel extends PMMLModel {

	// Map of names for wine data
	private static Map<String, String> names = createNamesMap();
	private static Map<String, String> createNamesMap() {
		Map<String, String> map = new HashMap<>();
		map.put("fixed acidity", "fixed_acidity");
		map.put("volatile acidity", "volatile_acidity");
		map.put("citric acid", "citric_acid");
		map.put("residual sugar", "residual_sugar");
		map.put("chlorides", "chlorides");
		map.put("free sulfur dioxide", "free_sulfur_dioxide");
		map.put("total sulfur dioxide", "total_sulfur_dioxide");
		map.put("density", "density");
		map.put("pH", "pH");
		map.put("sulphates", "sulphates");
		map.put("alcohol", "alcohol");
		return map;
	}

	// Arguments
	private Map<FieldName, FieldValue> arguments = new LinkedHashMap<>();
	/**
	 * Creates a new PMML model.
	 *
	 * @param input binary representation of PMML model.
	 */
	public WinePMMLModel(byte[] input) throws Throwable{

		super(input);
	}

	/**
	 * Score data.
	 *
	 * @param input object to score.
	 */
	@Override
	public Object score(Object input) {
		// Convert input
		Winerecord.WineRecord inputs = (Winerecord.WineRecord) input;
		// Clear arguments
		arguments.clear();
		// Populate arguments with incoming data
		for (InputField field : inputFields){
			arguments.put(field.getName(), field.prepare(getValueByName(inputs, field.getName().getValue())));
		}

		// Calculate Output using PMML evaluator
		Map<FieldName, ?> result = evaluator.evaluate(arguments);

		// Prepare output
		double rv = 0;
		Object tresult = result.get(tname);
		if (tresult instanceof Computable){
			String value = ((Computable) tresult).getResult().toString();
			rv = Double.parseDouble(value);
		}
		else {
			rv = (Double) tresult;
		}
		return rv;
	}

	/**
	 * Get variable value by  name.
	 *
	 * @param input record.
	 * @param name field name.
	 */
	private double getValueByName(Winerecord.WineRecord input, String name){
		Descriptors.FieldDescriptor descriptor =  input.getDescriptorForType().findFieldByName(names.get(name));
		return (double) input.getField(descriptor);
	}
}
