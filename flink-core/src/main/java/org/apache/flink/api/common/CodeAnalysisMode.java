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

package org.apache.flink.api.common;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Specifies to which extent user-defined functions are analyzed in order
 * to give the Flink optimizer an insight of UDF internals and inform
 * the user about common implementation mistakes.
 *
 * The analyzer gives hints about:
 *  - ForwardedFields semantic properties
 *  - Warnings if static fields are modified by a Function
 *  - Warnings if a FilterFunction modifies its input objects
 *  - Warnings if a Function returns null
 *  - Warnings if a tuple access uses a wrong index
 *  - Information about the number of object creations (for manual optimization)
 *
 * @deprecated The code analysis code has been removed and this enum has no effect.
 */
@PublicEvolving
@Deprecated
public enum CodeAnalysisMode {

	/**
	 * Code analysis does not take place.
	 */
	DISABLE,

	/**
	 * Hints for improvement of the program are printed to the log.
	 */
	HINT,

	/**
	 * The program will be automatically optimized with knowledge from code
	 * analysis.
	 */
	OPTIMIZE;

}
