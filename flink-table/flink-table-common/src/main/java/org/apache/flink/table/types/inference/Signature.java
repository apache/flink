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

package org.apache.flink.table.types.inference;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Describes the signature of a function. It is meant for representing information for debugging
 * purposes.
 *
 * <p>A signature is returned from {@link InputTypeStrategy#getExpectedSignatures(FunctionDefinition)}.
 */
@PublicEvolving
public final class Signature {

	private final List<Argument> arguments;

	private Signature(List<Argument> arguments) {
		this.arguments = Preconditions.checkNotNull(arguments, "Argument must not be null.");
	}

	/**
	 * Creates an immutable instance of {@link Signature}.
	 */
	public static Signature of(Argument... arguments) {
		return new Signature(Arrays.asList(arguments));
	}

	/**
	 * Creates an immutable instance of {@link Signature}.
	 */
	public static Signature of(List<Argument> arguments) {
		return new Signature(arguments);
	}

	public List<Argument> getArguments() {
		return arguments;
	}

	/**
	 * Representation of a single argument in a signature.
	 *
	 * <p>The type is represented as {@link String} in order to also express type families or varargs.
	 */
	public static final class Argument {

		private final @Nullable String name;

		private final String type;

		private Argument(@Nullable String name, String type) {
			this.name = name;
			this.type = Preconditions.checkNotNull(type);
		}

		/**
		 * Returns an instance of {@link Argument}.
		 */
		public static Argument of(String name, String type) {
			return new Argument(
				Preconditions.checkNotNull(name, "Name must not be null."),
				type);
		}

		/**
		 * Returns an instance of {@link Argument}.
		 */
		public static Argument of(String type) {
			return new Argument(null, type);
		}

		public Optional<String> getName() {
			return Optional.ofNullable(name);
		}

		public String getType() {
			return type;
		}
	}
}
