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

package org.apache.flink.table.runtime.sort;

import org.apache.flink.table.generated.RecordComparator;

/**
 * Example String {@link RecordComparator}.
 */
public class StringRecordComparator implements RecordComparator {

	@Override
	public int compare(org.apache.flink.table.dataformat.BaseRow o1,
			org.apache.flink.table.dataformat.BaseRow o2) {
		boolean null0At1 = o1.isNullAt(0);
		boolean null0At2 = o2.isNullAt(0);
		int cmp0 = null0At1 && null0At2 ? 0 : (null0At1 ? -1 : (null0At2 ? 1 : o1.getString(0).compareTo(o2.getString(0))));
		if (cmp0 != 0) {
			return cmp0;
		}
		return 0;
	}

}
