/**
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

package org.apache.flink.api.java.record.functions;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;

/**
 * The JoinFunction must implementation by functions of a {@link org.apache.flink.api.java.record.operators.JoinOperator}.
 * It resembles an equality join of both inputs on their key fields.
 */
public abstract class JoinFunction extends AbstractRichFunction implements FlatJoinFunction<Record, Record, Record> {
	
	private static final long serialVersionUID = 1L;
	
	@Override
	public abstract void join(Record value1, Record value2, Collector<Record> out) throws Exception;
}
