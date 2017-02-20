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

import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Hive UDF Wrapper which behaves as a Flink-table ScalarFunction.
 *
 * This class has to be implemented in Java. For scala will compile
 * <code> eval(args: Any*) </code> to <code>eval(args: Seq)</code>.
 * This will cause an exception in Janino compiler.
 */
public class HiveSimpleUDF extends ScalarFunction {

	private static Logger logger = LoggerFactory.getLogger(HiveSimpleUDF.class);

	public HiveSimpleUDF() {
	}

	public int eval(Object... args) {
		return args.length;
	}
}
