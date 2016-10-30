/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.scala;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.InvalidProgramException;

/**
 * Scala Object checker tries to verify if a class is implemented by
 * Scala Object
 */
@Internal
public class ScalaObjectChecker {
	public static boolean isPotentialScalaObject(Object o) {
		final Class<?> cls = o.getClass();
		String clsName = cls.getCanonicalName();
		if (clsName.length() > 0) {
			char lastChar = clsName.charAt(clsName.length() - 1);
			if (lastChar == '$') {
				return true;
			} else return false;
		}
		return false;
	}

	public static void assertScalaForbidScalaObjectFunction(Object o) {
		if (isPotentialScalaObject(o)) {
			String msg = "User defined function implemented by class " + o.getClass().getCanonicalName() +
				" might be implemented by a Scala Object,it is forbidden by Flink since concurrent modification risks.";
			throw new InvalidProgramException(msg);
		}
	}
}
