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

package org.apache.flink.formats.protobuf.utils;

import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.function.Function;

/**
 * Utils to verify the result of {@link TypeSerializerSchemaCompatibility}.
 */
public class SerializerSchemaCompatibilityUtils {

	public static Matcher<TypeSerializerSchemaCompatibility> isCompatibleAsIs() {
		return matcher(TypeSerializerSchemaCompatibility::isCompatibleAsIs, "compatible as is");
	}

	public static Matcher<TypeSerializerSchemaCompatibility> isCompatibleAfterMigration() {
		return matcher(TypeSerializerSchemaCompatibility::isCompatibleAfterMigration,
			"compatible after migration");
	}

	private static <T> Matcher<T> matcher(Function<T, Boolean> predicate, String message) {
		return new TypeSafeDiagnosingMatcher<T>() {

			@Override
			protected boolean matchesSafely(T item, Description mismatchDescription) {
				if (predicate.apply(item)) {
					return true;
				}
				mismatchDescription.appendText("not ").appendText(message);
				return false;
			}

			@Override
			public void describeTo(Description description) {
			}
		};
	}
}
