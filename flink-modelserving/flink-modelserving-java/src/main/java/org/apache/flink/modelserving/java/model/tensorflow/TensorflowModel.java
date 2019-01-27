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

import org.apache.flink.annotation.Public;
import org.apache.flink.model.Modeldescriptor;
import org.apache.flink.modelserving.java.model.Model;

import org.tensorflow.Graph;
import org.tensorflow.Session;

import java.util.Arrays;

/**
 * Base class for tensorflow (optimized) model processing.
 */
@Public
public abstract class TensorflowModel<RECORD, RESULT> implements Model<RECORD, RESULT> {
	// Tensorflow graph
	protected Graph graph = new Graph();
	// Tensorflow session
	protected Session session;
	// Byte array
	protected byte[] bytes;

	/**
	 * Creates a new tensorflow (optimized) model.
	 *
	 * @param input binary representation of tensorflow(optimized) model.
	 */
	public TensorflowModel(byte[] input) throws Throwable {
		if (input.length < 1) {
			throw new Exception("Empty graph data");
		}
		bytes = input;
		graph.importGraphDef(input);
		session = new Session(graph);
	}

	/**
	 * Clean up tensorflow model.
	 */
	@Override
	public void cleanup() {
		session.close();
		graph.close();
	}

	/**
	 * Get bytes representation of tensorflow model.
	 * @return binary representation of the tensorflow model.
	 */
	@Override
	public byte[] getBytes() {
		return bytes;
	}

	/**
	 * Get model'a type.
	 * @return tensorflow (optimized) model type.
	 */
	@Override
	public long getType() {

		return (long) Modeldescriptor.ModelDescriptor.ModelType.TENSORFLOW_VALUE;
	}

	/**
	 * Compare 2 tensorflow (optimized) models. They are equal if their binary content is same.
	 * @param obj other model.
	 * @return boolean specifying whether models are the same.
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TensorflowModel) {
			return Arrays.equals(((TensorflowModel) obj).getBytes(), bytes);
		}
		return false;
	}
}
