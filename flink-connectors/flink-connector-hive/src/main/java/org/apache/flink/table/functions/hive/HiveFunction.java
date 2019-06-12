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

package org.apache.flink.table.functions.hive;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.DataType;

/**
 * Interface for Hive simple udf, generic udf, and generic udtf.
 * TODO: Note: this is only a temporary interface for workaround when Flink type system and udf system
 * 	rework is not finished. Should adapt to Flink type system and Flink UDF framework later on.
 */
@Internal
public interface HiveFunction {

	/**
	 * Set arguments and argTypes for Function instance.
	 * In this way, the correct method can be really deduced by the function instance.
	 *
	 * @param constantArguments arguments of a function call (only literal arguments
	 *                  are passed, nulls for non-literal ones)
	 * @param argTypes types of arguments
	 */
	void setArgumentTypesAndConstants(Object[] constantArguments, DataType[] argTypes);

	/**
	 * Get result type by arguments and argTypes.
	 *
	 * <p>We can't use getResultType(Object[], Class[]). The Class[] is the classes of what
	 * is defined in eval(), for example, if eval() is "public Integer eval(Double)", the
	 * argTypes would be Class[Double]. However, in our wrapper, the signature of eval() is
	 * "public Object eval(Object... args)", which means we cannot get any info from the interface.
	 *
	 * @param constantArguments arguments of a function call (only literal arguments
	 *                  are passed, nulls for non-literal ones)
	 * @param argTypes types of arguments
	 * @return result type.
	 */
	DataType getHiveResultType(Object[] constantArguments, DataType[] argTypes);
}
