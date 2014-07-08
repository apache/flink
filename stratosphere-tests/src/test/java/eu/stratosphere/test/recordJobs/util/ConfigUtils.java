/***********************************************************************************************************************
 *
 * Copyright (C) 20102
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

package eu.stratosphere.test.recordJobs.util;

import java.util.HashSet;
import java.util.Set;

import eu.stratosphere.configuration.Configuration;

public class ConfigUtils {

	private ConfigUtils() {}

	public static int asInteger(String key, Configuration parameters) {
		int value = parameters.getInteger(key, -1);
		if (value == -1) {
			throw new IllegalStateException();
		}
		return value;
	}

	public static double asDouble(String key, Configuration parameters) {
		double value = parameters.getDouble(key, Double.NaN);
		if (Double.isNaN(value)) {
			throw new IllegalStateException();
		}
		return value;
	}

	public static long asLong(String key, Configuration parameters) {
		long value = parameters.getLong(key, Long.MIN_VALUE);
		if (value == Long.MIN_VALUE) {
			throw new IllegalStateException();
		}
		return value;
	}

	public static Set<Integer> asIntSet(String key, Configuration parameters) {
		String[] tokens = parameters.getString(key, "").split(",");
		Set<Integer> intSet = new HashSet<Integer>(tokens.length);
		for (String token : tokens) {
			intSet.add(Integer.parseInt(token));
		}
		return intSet;
	}
}
