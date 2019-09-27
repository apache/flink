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

package org.apache.flink.walkthrough.common.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * A user defined function for rounding timestamps down to
 * the nearest hour.
 */
@PublicEvolving
@SuppressWarnings("unused")
public class TruncateDateToHour extends ScalarFunction {

	private static final long serialVersionUID = 1L;

	private static final long ONE_HOUR = 60 * 60 * 1000;

	public long eval(long timestamp) {
		return timestamp - (timestamp % ONE_HOUR);
	}

	@Override
	public TypeInformation<?> getResultType(Class<?>[] signature) {
		return Types.SQL_TIMESTAMP;
	}
}
