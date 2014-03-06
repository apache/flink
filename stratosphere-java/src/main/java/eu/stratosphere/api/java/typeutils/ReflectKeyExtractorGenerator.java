/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.typeutils;

import java.io.Serializable;
import java.lang.reflect.Field;

import eu.stratosphere.api.java.functions.KeySelector;


/**
 *
 */
public class ReflectKeyExtractorGenerator {
	
	
	public static <IN, KEY> KeySelector<IN, KEY> generateKeyExtractor(TypeInformation<IN> type, String expression) {
		return null;
	}
	
	
	@SuppressWarnings("unused")
	private static final class DirectReflectKeyAccessor<IN, KEY> extends KeySelector<IN, KEY> implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final Field field;
		
		private DirectReflectKeyAccessor(Field field) {
			this.field = field;
			this.field.setAccessible(true);
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public KEY getKey(IN value) {
			try {
				return (KEY) field.get(value);
			} catch (Throwable t) {
				throw new RuntimeException("Reflection key accessor could not extract key: " + t.getMessage(), t);
			}
		}
	}
}
