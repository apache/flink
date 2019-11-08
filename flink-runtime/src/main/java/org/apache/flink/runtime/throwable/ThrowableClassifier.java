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

package org.apache.flink.runtime.throwable;

import java.util.Optional;

/**
 * Helper class, given a exception do the classification.
 */
public class ThrowableClassifier {

	/**
	 * Classify the exceptions by extracting the {@link ThrowableType} from a potential {@link ThrowableAnnotation}.
	 *
	 * @param cause the {@link Throwable} to classify.
	 * @return The extracted {@link ThrowableType} or ThrowableType.RecoverableError if there is no such annotation.
	 */
	public static ThrowableType getThrowableType(Throwable cause) {
		final ThrowableAnnotation annotation = cause.getClass().getAnnotation(ThrowableAnnotation.class);
		return annotation == null ? ThrowableType.RecoverableError : annotation.value();
	}

	/**
	 * Checks whether a throwable chain contains a specific throwable type and returns the corresponding throwable.
	 *
	 * @param throwable the throwable chain to check.
	 * @param throwableType the throwable type to search for in the chain.
	 * @return Optional throwable of the throwable type if available, otherwise empty
	 */
	public static Optional<Throwable> findThrowableOfThrowableType(Throwable throwable, ThrowableType throwableType) {
		if (throwable == null || throwableType == null) {
			return Optional.empty();
		}

		Throwable t = throwable;
		while (t != null) {
			final ThrowableAnnotation annotation = t.getClass().getAnnotation(ThrowableAnnotation.class);
			if (annotation != null && annotation.value() == throwableType) {
				return Optional.of(t);
			} else {
				t = t.getCause();
			}
		}

		return Optional.empty();
	}
}
