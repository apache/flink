/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.functions.utils;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Decimal util.
 */
public class DecimalUtils {
	public static boolean isDecimal(Object obj) {
		if (obj == null){
			return false;
		}
		if ((obj instanceof Long)
				|| (obj instanceof Integer)
				|| (obj instanceof Short)
				|| (obj instanceof Byte)
				|| (obj instanceof Float)
				|| (obj instanceof Double)
				|| (obj instanceof BigDecimal)
				|| (obj instanceof BigInteger)){
			return true;
		}
		if (obj instanceof String || obj instanceof Character){
			String s = obj.toString();
			if (s == null || "".equals(s)) {
				return false;
			}
			if (isInteger(s) || isLong(s) || isDouble(s)) {
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}

	}

	private static boolean isInteger(String s) {
		boolean flag = true;
		try {
			Integer.parseInt(s);
		} catch (NumberFormatException e) {
			flag = false;
		}
		return flag;
	}

	private static boolean isLong(String s) {
		boolean flag = true;
		try {
			Long.parseLong(s);
		} catch (NumberFormatException e) {
			flag = false;
		}
		return flag;
	}

	private static boolean isDouble(String s) {
		boolean flag = true;
		try {
			Double.parseDouble(s);
		} catch (NumberFormatException e) {
			flag = false;
		}
		return flag;
	}
}
