/***********************************************************************************************************************
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **********************************************************************************************************************/

package org.apache.flink.streaming.faulttolerance;

import java.util.HashMap;
import java.util.Map;

public enum FaultToleranceType {
	NONE(0), AT_LEAST_ONCE(1), EXACTLY_ONCE(2);

	public final int id;

	FaultToleranceType(int id) {
		this.id = id;
	}

	private static final Map<Integer, FaultToleranceType> map = new HashMap<Integer, FaultToleranceType>();
	static {
		for (FaultToleranceType type : FaultToleranceType.values())
			map.put(type.id, type);
	}

	public static FaultToleranceType from(int id) {
		return map.get(id);
	}
}