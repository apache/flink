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

import org.apache.flink.model.Modeldescriptor;
import org.apache.flink.modelserving.java.model.ModelFacroriesResolver;
import org.apache.flink.modelserving.java.model.ModelFactory;
import org.apache.flink.modelserving.wine.Winerecord;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of Factory resolver for testing.
 */
public class WineFactoryResolver implements ModelFacroriesResolver<Winerecord.WineRecord, Double> {

	private static final Map<Integer, ModelFactory> factories = new HashMap<Integer, ModelFactory>(){
		{
			put(Modeldescriptor.ModelDescriptor.ModelType.TENSORFLOW.getNumber(), WineTensorflowModelFactory.getInstance());
			put(Modeldescriptor.ModelDescriptor.ModelType.TENSORFLOWSAVED.getNumber(), WineTensorflowBundledModelFactory.getInstance());
		}
	};

	/**
	 * Get factory based on type.
	 *
	 * @param type model type.
	 * @return model factory.
	 */
	@Override
	public ModelFactory<Winerecord.WineRecord, Double> getFactory(int type) {
		return factories.get(type);
	}
}
