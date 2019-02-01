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

package org.apache.flink.table.utils;

/**
  * Utils for table sources and sinks.
  */
public final class TableConnectorUtil {

	/**
	 * Returns the table connector name used for log and web UI.
	 */
	public static String generateRuntimeName(Class<?> clazz, String[] fields) {
		String className = clazz.getSimpleName();
		if (null == fields) {
			return className + "(*)";
		} else {
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < fields.length; i++) {
				sb.append(fields[i]);
				if (i != fields.length - 1) {
					sb.append(", ");
				}
			}
			return className + "(" + sb.toString() + ")";
		}
	}
}
