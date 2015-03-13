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

package org.apache.flink.api.common.typeinfo;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * TypeInformation is the core class of Flink's type system. Flink requires a type information
 * for all types that are used as input or return type of a user function. This type information
 * class acts as the tool to generate serializers and comparators, and to perform semantic checks
 * such as whether the fields that are uses as join/grouping keys actually exist. 
 * <p>
 * The type information also bridges between the programming languages object model and a
 * logical flat schema. It maps fields from the types to to columns (fields) in a flat schema.
 * Not all fields from a type are mapped to a separate fields in the flat schema and 
 * often, entire types are mapped to one field. It is important to notice that the schema must
 * hold for all instances of a type. For that reason, elements in lists and arrays are not
 * assigned to individual fields, but the lists and arrays are considered to be one field in total,
 * to account for different lengths in the arrays.  
 * <ul>
 *   <li>Basic types are indivisible and are considered a single field.</li>
 *   <li>Arrays and collections are one field</li>
 *   <li>Tuples and case classes represent as many fields as the class has fields</li>
 *   <li></li>
 *   <li></li>
 *   <li></li>
 * </ul>
 * <p>
 * To represent this properly, each type has an <i>arity</i> (the number of fields it contains
 * directly), and a <i>total number of fields</i> (number of fields in the entire schema of this
 * type, including nested types).
 * <p>
 * Consider the example below:
 * <pre>{@code
 * public class InnerType {
 *   public int id;
 *   public String text;
 * }
 * 
 * public class OuterType {
 *   public long timestamp;
 *   public InnerType nestedType;
 * }
 * }</pre>
 * 
 * The types "id", "text", and "timestamp" are basic types that take up one field. The "InnerType"
 * has an arity of two, and also two fields totally. The "OuterType" has an arity of two fields,
 * and a total number of three fields ( it contains "id", "text", and "timestamp" through recursive flattening).
 *
 * @param <T> The type represented by this type information.
 */
public abstract class TypeInformation<T> implements Serializable {
	
	private static final long serialVersionUID = -7742311969684489493L;

	/**
	 * Checks if this type information represents a basic type.
	 * Basic types are defined in {@link BasicTypeInfo} and are primitives, their boxing types,
	 * Strings, Date, Void, ...
	 *  
	 * @return True, if this type information describes a basic type, false otherwise.
	 */
	public abstract boolean isBasicType();
	
	/**
	 * Checks if this type information represents a Tuple type.
	 * Tuple types are subclasses of the Java API tuples.
	 *  
	 * @return True, if this type information describes a tuple type, false otherwise.
	 */
	public abstract boolean isTupleType();
	
	/**
	 * Gets the arity of this type - the number of fields without nesting.
	 * 
	 * @return Gets the number of fields in this type without nesting.
	 */
	public abstract int getArity();
	
	/**
	 * Gets the number of logical fields in this type. This includes its nested and transitively nested
	 * fields, in the case of composite types. In the example below, the OuterType type has three
	 * fields in total.
	 * 
	 * 
	 * @return The number of fields in this type, including its sub-fields (for composite types) 
	 */
	public abstract int getTotalFields();
	
	/**
	 * Gets the class of the type represented by this type information.
	 *  
	 * @return The class of the type represented by this type information.
	 */
	public abstract Class<T> getTypeClass();

	/**
	 * Returns the generic parameters of this type.
	 *
	 * @return The list of generic parameters. This list can be empty.
	 */
	public List<TypeInformation<?>> getGenericParameters() {
		// Return an empty list as the default implementation
		return new LinkedList<TypeInformation<?>>();
	}

	/**
	 * Checks whether this type can be used as a key. As a bare minimum, types have
	 * to be hashable and comparable to be keys.
	 *  
	 * @return True, if the type can be used as a key, false otherwise.
	 */
	public abstract boolean isKeyType();
	
	/**
	 * Creates a serializer for the type. The serializer may use the ExecutionConfig
	 * for parameterization.
	 * 
	 * @param config The config used to parameterize the serializer.
	 * @return A serializer for this type.
	 */
	public abstract TypeSerializer<T> createSerializer(ExecutionConfig config);
}
