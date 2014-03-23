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

import org.apache.commons.lang3.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.common.InvalidProgramException;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.types.Value;


public abstract class TypeInformation<T> {
	
	public abstract boolean isBasicType();

	public abstract boolean isTupleType();
	
	public abstract int getArity();
	
	public abstract Class<T> getTypeClass();
	
	public abstract boolean isKeyType();
	
	public abstract TypeSerializer<T> createSerializer();
	
	protected static final Log LOG = LogFactory.getLog(TypeInformation.class);
	
	// --------------------------------------------------------------------------------------------
	// Static Utilities
	// --------------------------------------------------------------------------------------------
	
	@SuppressWarnings("unchecked")
	public static <X> TypeInformation<X> getForClass(Class<X> clazz) {
		Validate.notNull(clazz);
		
		// check for basic types
		{
			TypeInformation<X> basicTypeInfo = BasicTypeInfo.getInfoFor(clazz);
			if (basicTypeInfo != null) {
				return basicTypeInfo;
			}
		}
		
		// check for subclasses of Value
		if (Value.class.isAssignableFrom(clazz)) {
			Class<? extends Value> valueClass = clazz.asSubclass(Value.class);
			return (TypeInformation<X>) ValueTypeInfo.getValueTypeInfo(valueClass);
		}
		
		// check for subclasses of Tuple
		if (Tuple.class.isAssignableFrom(clazz)) {
			throw new IllegalArgumentException("Type information extraction for tuples cannot be done based on the class.");
		}
		
		// return a generic type
		return new GenericTypeInfo<X>(clazz);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <X> TypeInformation<X> getForObject(X value) {
		Validate.notNull(value);
		
		// check if we can extract the types from tuples, otherwise work with the class
		if (value instanceof Tuple) {
			Tuple t = (Tuple) value;
			int numFields = t.getArity();
			
			TypeInformation<?>[] infos = new TypeInformation[numFields];
			for (int i = 0; i < numFields; i++) {
				Object field = t.getField(i);
				
				if (field == null) {
					throw new InvalidProgramException("Automatic type extraction is not possible on canidates with null values. " +
							"Please specify the types directly.");
				}
				
				infos[i] = getForObject(field);
			}
			
			
			return (TypeInformation<X>) new TupleTypeInfo(value.getClass(), infos);
		}
		else {
			return getForClass((Class<X>) value.getClass());
		}
	}
}
