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

import org.apache.flink.modelserving.java.model.tensorflow.TensorflowModel;

import org.tensorflow.Graph;
import org.tensorflow.Session;

/**
 * Implementation of tensorflow (optimized) model for testing.
 */
public class SimpleTensorflowModel extends TensorflowModel<Double, Double> {

	/**
	 * Creates a new tensorflow (optimized) model.
	 *
	 * @param inputStream binary representation of tensorflow (optimized) model.
	 */
	public SimpleTensorflowModel(byte[] inputStream) throws Throwable {
		super(inputStream);
	}

	/**
	 * Score data.
	 *
	 * @param input object to score.
	 */
	@Override
	public Double score(Double input) {
        // Just for test
		return null;
	}

    // Getters for validation
	/**
	 * Get tensorflow session.
	 * @return tensorflow session.
	 */
	public Session getSession() {
		return session;
	}

	/**
	 * Get tensorflow graph.
	 * @return tensorflow graph.
	 */
	public Graph getGrapth() {
		return graph;
	}
}
