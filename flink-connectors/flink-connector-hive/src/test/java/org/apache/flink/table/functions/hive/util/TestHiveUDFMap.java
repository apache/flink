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

package org.apache.flink.table.functions.hive.util;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

/**
 * Test map in Hive UDF.
 */
public class TestHiveUDFMap extends UDF {

	public String evaluate(Map<String, String> a) {
		if (a == null) {
			return null;
		}
		ArrayList<String> r = new ArrayList<String>(a.size());
		for (Map.Entry<String, String> entry : a.entrySet()) {
			r.add("(" + entry.getKey() + ":" + entry.getValue() + ")");
		}
		Collections.sort(r);

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < r.size(); i++) {
			sb.append(r.get(i));
		}
		return sb.toString();
	}

}
