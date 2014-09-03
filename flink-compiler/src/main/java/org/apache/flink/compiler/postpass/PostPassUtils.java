/**
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

package org.apache.flink.compiler.postpass;

import org.apache.flink.compiler.CompilerException;
import org.apache.flink.types.Key;


public class PostPassUtils {

	public static <X> Class<? extends Key<?>>[] getKeys(AbstractSchema<Class< ? extends X>> schema, int[] fields) throws MissingFieldTypeInfoException {
		@SuppressWarnings("unchecked")
		Class<? extends Key<?>>[] keyTypes = new Class[fields.length];
		
		for (int i = 0; i < fields.length; i++) {
			Class<? extends X> type = schema.getType(fields[i]);
			if (type == null) {
				throw new MissingFieldTypeInfoException(i);
			} else if (Key.class.isAssignableFrom(type)) {
				@SuppressWarnings("unchecked")
				Class<? extends Key<?>> keyType = (Class<? extends Key<?>>) type;
				keyTypes[i] = keyType;
			} else {
				throw new CompilerException("The field type " + type.getName() +
						" cannot be used as a key because it does not implement the interface 'Key'");
			}
		}
		
		return keyTypes;
	}
}
