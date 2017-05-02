/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.flink.python.api.types;

/**
 * Container for serialized python objects, generally assumed to be custom objects.
 */
public class CustomTypeWrapper {
	private final byte typeID;
	private final byte[] data;

	public CustomTypeWrapper(byte typeID, byte[] data) {
		this.typeID = typeID;
		this.data = data;
	}

	public byte getType() {
		return typeID;
	}

	public byte[] getData() {
		return data;
	}
}
