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

package org.apache.flink.table.functions;

/**
 * Definition of an user-defined function. Instances of this class provide all details necessary to
 * validate a function call and perform planning.
 *
 * <p>Compared to {@link FunctionDefinition}, this definition provides a runtime implementation.
 */
public interface UserDefinedFunctionDefinition extends FunctionDefinition {

	/**
	 * Creates a runtime implementation for this definition.
	 *
	 * <p>This method allows for lazy instantiation of user-defined functions.
	 */
	UserDefinedFunction createImplementation();
}
