/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.streaming.util.serialization;

import java.io.Serializable;

import org.apache.flink.types.TypeInformation;

public abstract class TypeSerializerWrapper<T>
		implements Serializable {
	private static final long serialVersionUID = 1L;

	protected transient TypeInformation<T> typeInfo = null;
	
	public TypeInformation<T> getTypeInfo() {
		if (typeInfo == null) {
			throw new RuntimeException("There is no TypeInformation in the wrapper");
		}
		return typeInfo;
	}

	protected abstract void setTypeInfo();
}