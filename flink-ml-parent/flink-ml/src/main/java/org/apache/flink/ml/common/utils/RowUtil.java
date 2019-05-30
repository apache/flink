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

import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

/**
 * Row Util.
 */
public class RowUtil {
	public static Row remove(Row rec, int idx) {
		int n1 = rec.getArity();
		Row ret = new Row(n1 - 1);
		for (int i = 0; i < n1; ++i) {
			if (i < idx) {
				ret.setField(i, rec.getField(i));
			} else if (i > idx) {
				ret.setField(i - 1, rec.getField(i));
			}
		}
		return ret;
	}

	public static Row merge(Row rec1, Object obj) {
		int n1 = rec1.getArity();
		Row ret = new Row(n1 + 1);
		for (int i = 0; i < n1; ++i) {
			ret.setField(i, rec1.getField(i));
		}
		ret.setField(n1, obj);
		return ret;
	}

	public static Row merge(Object obj, Row rec1) {
		int n1 = rec1.getArity();
		Row ret = new Row(n1 + 1);
		ret.setField(0, obj);
		for (int i = 0; i < n1; ++i) {
			ret.setField(i + 1, rec1.getField(i));
		}
		return ret;
	}

	public static Row merge(Row rec1, Row rec2) {
		int n1 = rec1.getArity();
		int n2 = rec2.getArity();
		Row ret = new Row(n1 + n2);
		for (int i = 0; i < n1; ++i) {
			ret.setField(i, rec1.getField(i));
		}
		for (int i = 0; i < n2; ++i) {
			ret.setField(i + n1, rec2.getField(i));
		}
		return ret;
	}

	public static String formatTitle(String[] colNames) {
		StringBuilder sbd = new StringBuilder();
		StringBuilder sbd1 = new StringBuilder();

		for (int i = 0; i < colNames.length; ++i) {
			if (i > 0) {
				sbd.append("|");
				sbd1.append("|");
			}

			sbd.append(colNames[i]);

			int t = null == colNames[i] ? 4 : colNames[i].length();
			for (int j = 0; j < t; j++) {
				sbd1.append("-");
			}
		}

		return sbd.toString() + "\r\n" + sbd1.toString();
	}

	public static String formatRows(Row row) {
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < row.getArity(); ++i) {
			if (i > 0) {
				sb.append("|");
			}

			sb.append(StringUtils.arrayAwareToString(row.getField(i)));
		}

		return sb.toString();
	}

}
