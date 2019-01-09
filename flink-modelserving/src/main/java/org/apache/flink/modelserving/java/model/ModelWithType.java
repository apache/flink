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
 * Representation of the model serving statistics.
 */
public class ModelWithType {

	// Is model current
	private boolean current;
	// Model data type
	private String dataType;
	// MOdel
	private Optional<Model> model;

	/**
	 * Model with type default constructor.
	 */
	public ModelWithType(){
		current = false;
		dataType = "";
		this.model = Optional.empty();
	}

	/**
	 * Create Model with type.
	 *
	 * @param current Model currently in use.
	 * @param dataType Model data type.
	 * @param model model itself.
	 */
	public ModelWithType(boolean current, String dataType, Optional<Model> model){
		this.current = current;
		this.dataType = dataType;
		this.model = model;
	}

	/**
	 * Get model's state.
	 *
	 * @return model's state.
	 */
	public boolean isCurrent() {
		return current;
	}

	/**
	 * Set current model state.
	 *
	 * @param current current model state.
	 */
	public void setCurrent(boolean current) {
		this.current = current;
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
	 * Set model data type.
	 *
	 * @param dataType model data type.
	 */
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	/**
	 * Get model.
	 *
	 * @return model.
	 */
	public Optional<Model> getModel() {
		return model;
	}

	/**
	 * Set model.
	 *
	 * @param model model.
	 */
	public void setModel(Optional<Model> model) {
		this.model = model;
	}

	/**
	 * Compare two models with type.
	 *
	 * @param obj another model with type.
	 * @return boolean, specifying whether they are equal.
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ModelWithType) {
			ModelWithType other = (ModelWithType) obj;
			boolean modelEqual;
			if (model.isPresent()){
				if (other.getModel().isPresent()) {
					modelEqual = model.get().equals(other.getModel().get());
				}
				else {
					modelEqual = false;
				}
			} else {
				if (other.getModel().isPresent()) {
					modelEqual = false;
				}
				else {
					modelEqual = true;
				}
			}
			return modelEqual && (dataType.equals(other.getDataType())) && (current == other.isCurrent());
		}
		return false;
	}

	/**
	 * Get model with type as a String.
	 *
	 * @return model with type as a String.
	 */
	@Override
	public String toString() {
		return "ModelWithType{" +
			"current=" + current +
			", dataType='" + dataType + '\'' +
			", model=" + model +
			'}';
	}
}
