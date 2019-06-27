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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.FunctionDefinition;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * This class is meant for debugging purposes only. It represents a signature
 * returned in {@link InputTypeValidator#getExpectedSignatures(FunctionDefinition)}.
 */
@Internal
public class Signature {
	private final List<Argument> arguments = new ArrayList<>();

	public static Signature newInstance() {
		return new Signature();
	}

	public Signature arg(String type) {
		arguments.add(new Argument(null, type));
		return this;
	}

	public Signature arg(String name, String type) {
		arguments.add(new Argument(name, type));
		return this;
	}

	public List<Argument> getArguments() {
		return arguments;
	}

	/**
	 * Representation of a single argument in a signature.
	 */
	public static class Argument {

		@Nullable
		private final String name;
		private final String type;

		private Argument(@Nullable String name, String type) {
			this.name = name;
			this.type = type;
		}

		public Optional<String> getName() {
			return Optional.ofNullable(name);
		}

		public String getType() {
			return type;
		}
	}

	private Signature() {
	}
}
