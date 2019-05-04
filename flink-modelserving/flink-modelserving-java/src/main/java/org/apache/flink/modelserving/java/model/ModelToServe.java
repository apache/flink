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

import java.io.Serializable;

/**
 * Internal generic representation for model to serve.
 */
public class ModelToServe implements Serializable {

	// Name
	private String name;
	// Description
	private String description;
	// Model type
	private Modeldescriptor.ModelDescriptor.ModelType modelType;
	// Binary representation
	private byte[] modelData;
	// Model data location
	private String modelDataLocation;
	// Data type
	private String dataType;

	/**
	 * Creates a new ModelToServe based on results of parsing modeldescriptor.proto.
	 *
	 * @param name Model name.
	 * @param description Model description.
	 * @param modelType Type of the model.
	 * @param dataContent Binary content of model, if the data is passed by value.
	 * @param modelDataLocation Location, if the model is passed by reference.
	 * @param dataType Type (ID) of data for which model is used.
	 */
	public ModelToServe(String name, String description, Modeldescriptor.ModelDescriptor.ModelType modelType,
						byte[] dataContent, String modelDataLocation, String dataType){
		this.name = name;
		this.description = description;
		this.modelType = modelType;
		this.modelData = dataContent;
		this.modelDataLocation = modelDataLocation;
		this.dataType = dataType;
	}

	/**
	 * Get model's name.
	 *
	 * @return model's name.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Get model's description.
	 *
	 * @return model's description.
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * Get model's type.
	 *
	 * @return model's type.
	 */
	public Modeldescriptor.ModelDescriptor.ModelType getModelType() {
		return modelType;
	}

	/**
	 * Get model's data type.
	 *
	 * @return model's data type.
	 */
	public String getDataType() {
		return dataType;
	}

	/**
	 * Get model's binary data.
	 *
	 * @return model's binary data.
	 */
	public byte[] getModelData() {
		return modelData;
	}

	/**
	 * Get model's location.
	 *
	 * @return model's location.
	 */
	public String getModelDataLocation() {
		return modelDataLocation;
	}

	/**
	 * Get intermediary model representation as a String.
	 *
	 * @return model's representation as a String.
	 */
	@Override
	public String toString() {
		return "ModelToServe{" +
			"name='" + name + '\'' +
			", description='" + description + '\'' +
			", modelType=" + modelType +
			", dataType='" + dataType + '\'' +
			'}';
	}
}
