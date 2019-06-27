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

package org.apache.flink.table.types.inference.validators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeValidator;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.Signature.Argument;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Validator that does not perform any validation and always passes.
 */
@Internal
public class PassingTypeValidator implements InputTypeValidator {

	private static final PassingArgumentCount PASSING_ARGUMENT_COUNT = new PassingArgumentCount();

	@Override
	public ArgumentCount getArgumentCount() {
		return PASSING_ARGUMENT_COUNT;
	}

	@Override
	public boolean validate(CallContext callContext, boolean throwOnFailure) {
		return true;
	}

	@Override
	public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
		return Collections.singletonList(Signature.of(Argument.of("*")));
	}

	private static class PassingArgumentCount implements ArgumentCount {

		@Override
		public boolean isValidCount(int count) {
			return true;
		}

		@Override
		public Optional<Integer> getMinCount() {
			return Optional.empty();
		}

		@Override
		public Optional<Integer> getMaxCount() {
			return Optional.empty();
		}
	}
}
