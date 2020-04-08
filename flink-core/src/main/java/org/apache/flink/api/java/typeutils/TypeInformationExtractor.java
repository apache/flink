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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.lang.reflect.Type;
import java.util.List;

/**
 * The {@link TypeExtractor} uses this interface to extract the {@link TypeInformation}.
 *
 * @param <T> the type the extractor would create for
 */
public interface TypeInformationExtractor<T> {

	/**
	 * @return the types that this extractor responses for extracting {@link TypeInformation}.
	 */
	List<Type> getTypeHandled();

	/**
	 * Extract {@link TypeInformation} for the given type.
	 * @param type the type needed to extract the {@link TypeInformation}
	 * @param context use to create {@link TypeInformation} for the generic parameters or the fields
	 * @return the {@link TypeInformation} of the given type
	 */
	TypeInformation<? extends T> create(Type type, Context context);

	/**
	 * The context is used to create the type information.
	 */
	interface Context {
		/**
		 * Create the {@link TypeInformation} for the give type.
		 * @param type the type needed to create the {@link TypeInformation}
		 * @return the {@link TypeInformation} of the given type
		 */
		TypeInformation<?> create(Type type);
	}
}
