/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.HashSet;
import java.util.Set;

/**
 * Array Util.
 */
public class ArrayUtil {

	public static long max(long[] counts) {
		long max = Long.MIN_VALUE;
		for (int i = 0; i < counts.length; i++) {
			if (max < counts[i]) {
				max = counts[i];
			}
		}
		return max;
	}

	public static long min(long[] counts) {
		long min = Long.MAX_VALUE;
		for (int i = 0; i < counts.length; i++) {
			if (min > counts[i]) {
				min = counts[i];
			}
		}
		return min;
	}

	public static double max(double[] counts) {
		double max = Double.MIN_VALUE;
		for (int i = 0; i < counts.length; i++) {
			if (max < counts[i]) {
				max = counts[i];
			}
		}
		return max;
	}

	public static double min(double[] counts) {
		double min = Double.MAX_VALUE;
		for (int i = 0; i < counts.length; i++) {
			if (min > counts[i]) {
				min = counts[i];
			}
		}
		return min;
	}

	public static TypeInformation <?>[] arrayMerge(TypeInformation <?> type1, TypeInformation <?>[] types2) {
		return arrayMerge(new TypeInformation <?>[] {type1}, types2);
	}

	public static TypeInformation <?>[] arrayMerge(TypeInformation <?>[] types1, TypeInformation <?> type2) {
		return arrayMerge(types1, new TypeInformation <?>[] {type2});
	}

	public static TypeInformation <?>[] arrayMerge(TypeInformation <?>[] types1, TypeInformation <?>[] types2) {
		if (null == types1 || null == types2) {
			throw new RuntimeException();
		}
		int n1 = types1.length;
		int n2 = types2.length;
		TypeInformation <?>[] r = new TypeInformation <?>[n1 + n2];
		for (int i = 0; i < n1; i++) {
			r[i] = types1[i];
		}
		for (int i = 0; i < n2; i++) {
			r[n1 + i] = types2[i];
		}
		return r;
	}

	public static String[] arrayMerge(String str1, String strs2) {
		return arrayMerge(new String[] {str1}, strs2);
	}

	public static String[] arrayMerge(String str1, String[] strs2) {
		return arrayMerge(new String[] {str1}, strs2);
	}

	public static String[] arrayMerge(String[] strs1, String str2) {
		return arrayMerge(strs1, new String[] {str2});
	}

	public static String[] arrayMerge(String[] strs1, String[] strs2) {
		if (null == strs1 || null == strs2) {
			throw new RuntimeException();
		}
		int n1 = strs1.length;
		int n2 = strs2.length;
		String[] r = new String[n1 + n2];
		for (int i = 0; i < n1; i++) {
			r[i] = strs1[i];
		}
		for (int i = 0; i < n2; i++) {
			r[n1 + i] = strs2[i];
		}
		return r;
	}

	public static String[] arrayMergeDistinct(String[] strs1, String[] strs2) {
		if (null == strs1 || null == strs2) {
			throw new RuntimeException();
		}
		Set <String> sets = new HashSet <>();

		for (int i = 0; i < strs1.length; i++) {
			sets.add(strs1[i]);
		}
		for (int i = 0; i < strs2.length; i++) {
			sets.add(strs2[i]);
		}
		return sets.toArray(new String[0]);
	}

	public static String array2str(String[] strs, String delim) {
		if (null == strs) {
			throw new RuntimeException();
		}
		StringBuilder sbd = new StringBuilder();
		sbd.append(strs[0]);
		for (int i = 1; i < strs.length; i++) {
			sbd.append(delim).append(strs[i]);
		}
		return sbd.toString();
	}

	public static String[] arraySub(String[] strs, String[] excludeStrs) {
		if (excludeStrs == null || excludeStrs.length == 0
			|| strs == null || strs.length == 0) {
			return strs;
		}

		String[] strDeepCopy = new String[strs.length];
		System.arraycopy(strs, 0, strDeepCopy, 0, strs.length);

		int i1 = 0;
		for (int i = 0; i < strDeepCopy.length; ++i) {
			int j = 0;
			for (; j < excludeStrs.length; ++j) {
				if (strDeepCopy[i].equals(excludeStrs[j])) {
					break;
				}
			}

			strDeepCopy[i1] = strDeepCopy[i];

			if (j == excludeStrs.length) {
				i1++;
			}
		}

		String[] ret = new String[i1];
		System.arraycopy(strDeepCopy, 0, ret, 0, i1);
		return ret;
	}
}
