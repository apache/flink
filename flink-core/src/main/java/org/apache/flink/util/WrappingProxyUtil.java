/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import org.apache.flink.annotation.Internal;

/**
 * Utilits for working with {@link WrappingProxy}.
 */
@Internal
public final class WrappingProxyUtil {

	private WrappingProxyUtil() {
		throw new AssertionError();
	}

	@SuppressWarnings("unchecked")
	public static <T> T stripProxy(T object) {

		T previous = null;

		while (object instanceof WrappingProxy && previous != object) {
			previous = object;
			object = ((WrappingProxy<T>) object).getWrappedDelegate();
		}

		return object;
	}
}
