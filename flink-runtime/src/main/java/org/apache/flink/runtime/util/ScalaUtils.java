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

package org.apache.flink.runtime.util;

import java.util.Optional;

import scala.Option;

/**
 * Utilities to convert Scala types into Java types.
 */
public class ScalaUtils {

	/**
	 * Converts a Scala {@link Option} to a {@link Optional}.
	 *
	 * @param scalaOption to convert into ta Java {@link Optional}
	 * @param <T> type of the optional value
	 * @return Optional of the given option
	 */
	public static <T> Optional<T> toJava(Option<T> scalaOption) {
		if (scalaOption.isEmpty()) {
			return Optional.empty();
		} else {
			return Optional.ofNullable(scalaOption.get());
		}
	}

	private ScalaUtils() {}
}
