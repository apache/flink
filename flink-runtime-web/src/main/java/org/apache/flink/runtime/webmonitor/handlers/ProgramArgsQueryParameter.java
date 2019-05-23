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

package org.apache.flink.runtime.webmonitor.handlers;

import java.io.File;

/**
 * Query parameter specifying the arguments for the program.
 * @deprecated please, use {@link JarRequestBody#FIELD_NAME_PROGRAM_ARGUMENTS_LIST}
 * @see org.apache.flink.client.program.PackagedProgram#PackagedProgram(File, String, String...)
 */
@Deprecated
public class ProgramArgsQueryParameter extends StringQueryParameter {

	public ProgramArgsQueryParameter() {
		super("program-args", MessageParameterRequisiteness.OPTIONAL);
	}

	@Override
	public String getDescription() {
		return String.format("Deprecated, please use '%s' instead. " +
			"String value that specifies the arguments for the program or plan",
			ProgramArgQueryParameter.PROGRAM_ARG_PARAMETER_NAME);
	}
}
