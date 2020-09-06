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

package org.apache.flink.util;

import org.apache.flink.annotation.Internal;

/**
 * Utility class for Java arrays.
 */
@Internal
public final class ArrayUtils {

	public static String[] concat(String[] array1, String[] array2) {
		if (array1.length == 0) {
			return array2;
		}
		if (array2.length == 0) {
			return array1;
		}
		String[] resultArray = new String[array1.length + array2.length];
		System.arraycopy(array1, 0, resultArray, 0, array1.length);
		System.arraycopy(array2, 0, resultArray, array1.length, array2.length);
		return resultArray;
	}
}
