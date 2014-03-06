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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.common.typeutils.Serializer;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.types.Value;


public abstract class TypeInformation<T> {
	
	public abstract boolean isBasicType();

	public abstract boolean isTupleType();
	
	public abstract int getArity();
	
	public abstract Class<T> getTypeClass();
	
	public abstract boolean isKeyType();
	
	public abstract Serializer<T> createSerializer();
	
	protected static final Log LOG = LogFactory.getLog(TypeInformation.class);
	
	// --------------------------------------------------------------------------------------------
	// Static Utilities
	// --------------------------------------------------------------------------------------------
	
	@SuppressWarnings("unchecked")
	public static <X> TypeInformation<X> getForClass(Class<X> clazz) {
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
			LOG.warn("TypeInformation extraction from class for Tuples not supported.");
		}
		
		// return a generic type
		return new GenericTypeInfo<X>(clazz);
	}	
	
}
