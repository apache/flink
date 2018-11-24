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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.utils.EncodingUtils;

import java.io.Serializable;

/**
 * Base class for all user-defined functions such as scalar functions, table functions,
 * or aggregation functions.
 */
@PublicEvolving
public abstract class UserDefinedFunction implements Serializable {
	/**
	 * Setup method for user-defined function. It can be used for initialization work.
	 * By default, this method does nothing.
	 */
	public void open(FunctionContext context) throws Exception {

	}

	/**
	 * Tear-down method for user-defined function. It can be used for clean up work.
	 * By default, this method does nothing.
	 */
	public void close() throws Exception {

	}

	/**
	 * @return true if and only if a call to this function is guaranteed to always return
	 * the same result given the same parameters; true is assumed by default
	 * if user's function is not pure functional, like random(), date(), now()...
	 * isDeterministic must return false
	 */
	public boolean isDeterministic() {
		return true;
	}

	public String functionIdentifier() {
		String md5 = EncodingUtils.hex(EncodingUtils.md5(EncodingUtils.encodeObjectToString(this)));
		return getClass().getCanonicalName().replace('.', '$').concat("$").concat(md5);
	}

	/**
	 * Returns the name of the UDF that is used for plan explain and logging.
	 */
	@Override
	public String toString() {
		return getClass().getSimpleName();
	}
}
