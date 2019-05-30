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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Common utils.
 */
public class Util {

	/**
	 * if feaure.
	 *
	 * @param featureColNames
	 * @param tensorColName
	 * @return
	 */
	public static boolean isVector(String[] featureColNames, String tensorColName) {
		if (featureColNames != null && featureColNames.length != 0) {
			return false;
		} else if (tensorColName != null) {
			return true;
		} else {
			throw new RuntimeException("feature col or tensor must exist.");
		}
	}

	public static int[] getVectorColIdxs(
		String[] featureColNames, String tensorColName, String[] colNames, TypeInformation <?>[] colTypes) {
		int[] featureIdxs = null;
		int featureLength = 1;
		boolean isVector = isVector(featureColNames, tensorColName);
		if (!isVector) {
			featureLength = featureColNames.length;
			featureIdxs = new int[featureLength];
			for (int i = 0; i < featureLength; ++i) {
				featureIdxs[i] = TableUtil.findIndexFromName(colNames, featureColNames[i]);
				if (featureIdxs[i] < 0) {
					throw new RuntimeException("col name not exist! " + featureColNames[i]);
				}
				if (colTypes[featureIdxs[i]] != Types.DOUBLE) {
					throw new RuntimeException("col type must be double when table! "
						+ featureColNames[i] + " " + colTypes[featureIdxs[i]]);
				}
			}
		} else {
			featureIdxs = new int[1];
			featureIdxs[0] = TableUtil.findIndexFromName(colNames, tensorColName);
			if (featureIdxs[0] < 0) {
				throw new RuntimeException("col name not exist! " + tensorColName);
			}
			if (colTypes[featureIdxs[0]] != Types.STRING) {
				throw new RuntimeException("col type must be string when tensor! " + tensorColName);
			}
		}
		return featureIdxs;
	}

	public static double getDoubleValue(Object obj, Class type) {
		if (Double.class == type) {
			return ((Double) obj).doubleValue();
		} else if (Integer.class == type) {
			return ((Integer) obj).doubleValue();
		} else if (Long.class == type) {
			return ((Long) obj).doubleValue();
		} else if (Float.class == type) {
			return ((Float) obj).doubleValue();
		} else if (Boolean.class == type) {
			return (Boolean) obj ? 1.0 : 0.0;
		} else if (java.sql.Date.class == type) {
			return (double) ((java.sql.Date) obj).getTime();
		} else {
			System.out.println("type: " + type.getName());
			throw new RuntimeException("Not implemented yet!");
		}
	}

	public static double getDoubleValue(Object obj) {
		return getDoubleValue(obj, obj.getClass());
	}

	public static void checkEmpty(String[] vec, String name) {
		if (vec == null || vec.length == 0) {
			throw new RuntimeException(name + " must be set.");
		}
	}

	public static TypeInformation[] checkNumberType(TableSchema schema, String[] selectedColNames) {
		TypeInformation[] colTypes = new TypeInformation[selectedColNames.length];
		for (int i = 0; i < selectedColNames.length; i++) {
			TypeInformation <?> colType = schema.getFieldType(selectedColNames[i]).get();
			colTypes[i] = colType;
			if (colType != Types.DOUBLE && colType != Types.LONG && colType != Types.INT) {
				throw new RuntimeException("col type must be double,long or int.");
			}
		}
		return colTypes;
	}

	public static int[] getIdxs(String[] vec, String[] vals) {
		if (vec == null) {
			return null;
		}
		int[] idxs = new int[vec.length];
		for (int i = 0; i < vals.length; i++) {
			idxs[i] = TableUtil.getColIndex(vec, vals[i]);
		}
		return idxs;
	}

	public static List <String> sliceString(String str, int segmentSize) {
		List <String> list = new ArrayList <>();
		if (null == str || str.length() == 0) {
			return list;
		}
		if (str.length() <= segmentSize) {
			list.add(str);
			return list;
		}
		int n = str.length();
		long cur = 0;
		while (cur < n) {
			String s = str.substring((int) cur, Math.min((int) (cur + segmentSize), n));
			list.add(s);
			cur += segmentSize;
		}
		return list;
	}

	public static String mergeString(List <String> strings) {
		StringBuilder sbd = new StringBuilder();
		strings.forEach(sbd::append);
		return sbd.toString();
	}

	public static Map <String, String> parseArgs(String configFilePath) {
		try {
			Map <String, String> params = new HashMap <>();
			Files.lines(new File(configFilePath).toPath())
				.forEach(s -> {
					int idx = s.trim().indexOf("=");
					String key = s.trim().substring(0, idx).trim();
					String val = s.trim().substring(idx + 1).trim();
					params.put(key, val);
				});
			return params;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
