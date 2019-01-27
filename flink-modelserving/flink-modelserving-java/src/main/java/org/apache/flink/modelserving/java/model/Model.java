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

import org.apache.flink.annotation.Public;

import java.io.Serializable;

/**
 * Base interface for all models, describing base methods that every model has to implement.
 * Every concrete model implementation has to implement this interface
 */
@Public
public interface Model<RECORD, RESULT> extends Serializable {
	/**
	 * Score data using model.
	 * @param input
	 *            Input data to score
	 */
	RESULT score(RECORD input);

	/**
	 * Clean up model data.
	 */
	void cleanup();

	/**
	 * Get model representation as byte array.
	 * @return byte representation of the model
	 */
	byte[] getBytes();

	/**
	 * Get model type.
	 * @return model type
	 */
	long getType();
}
