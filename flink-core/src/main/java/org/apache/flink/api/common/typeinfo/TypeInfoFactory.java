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

import org.apache.flink.annotation.Public;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Base class for implementing a type information factory. A type information factory allows for
 * plugging-in user-defined {@link TypeInformation} into the Flink type system. The factory is
 * called during the type extraction phase if the corresponding type has been annotated with
 * {@link TypeInfo}. In a hierarchy of types the closest factory will be chosen while traversing
 * upwards.
 *
 * @param <T> type for which {@link TypeInformation} is created
 */
@Public
public abstract class TypeInfoFactory<T> {

	/**
	 * Creates type information for the type the factory is targeted for. The parameters provide
	 * additional information about the type itself as well as the type's generic type parameters.
	 *
	 * @param t the exact type the type information is created for; might also be a subclass of &lt;T&gt;
	 * @param genericParameters mapping of the type's generic type parameters to type information
	 *                          extracted with Flink's type extraction facilities; null values
	 *                          indicate that type information could not be extracted for this parameter
	 * @return type information for the type the factory is targeted for
	 */
	public abstract TypeInformation<T> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters);

}
