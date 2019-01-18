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

import com.google.protobuf.Descriptors;

import java.util.List;

/**
 * Tensorflow bundled Field definition.
 */
public class TField {

	// Field name
	private String name;
	// Field type
	private Descriptors.EnumValueDescriptor type;
	// Field shape
	private List<Integer> shape;

	/**
	 * Creates a new Tensorflow Field.
	 *
	 * @param name field`s name.
	 * @param type field`s type.
	 * @param shape field`s shape.
	 */
	public TField(String name, Descriptors.EnumValueDescriptor type, List<Integer> shape){
		this.name = name;
		this.type = type;
		this.shape = shape;
	}

	/**
	 * Get field's name.
	 * @return field's name.
	 */
	public String getName() {

		return name;
	}

	/**
	 * Get field's type.
	 * @return field's type.
	 */
	public Descriptors.EnumValueDescriptor getType() {

		return type;
	}

	/**
	 * Get field's shape.
	 * @return field's shape.
	 */
	public List<Integer> getShape() {

		return shape;
	}
}
