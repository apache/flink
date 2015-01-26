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

package org.apache.flink.api.java;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;

import java.util.List;

public class Utils {

	public static String getCallLocationName() {
		return getCallLocationName(4);
	}

	public static String getCallLocationName(int depth) {
		StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();

		if (stackTrace.length < depth) {
			return "<unknown>";
		}

		StackTraceElement elem = stackTrace[depth];

		return String.format("%s(%s:%d)", elem.getMethodName(), elem.getFileName(), elem.getLineNumber());
	}

	/**
	 * Returns all GenericTypeInfos contained in a composite type.
	 *
	 * @param typeInfo
	 * @return
	 */
	public static void getContainedGenericTypes(CompositeType typeInfo, List<GenericTypeInfo<?>> target) {
		for(int i = 0; i < typeInfo.getArity(); i++) {
			TypeInformation<?> type = typeInfo.getTypeAt(i);
			if(type instanceof CompositeType) {
				getContainedGenericTypes((CompositeType) type, target);
			} else if(type instanceof GenericTypeInfo) {
				if(!target.contains(type)) {
					target.add((GenericTypeInfo<?>) type);
				}
			}
		}
	}
}
