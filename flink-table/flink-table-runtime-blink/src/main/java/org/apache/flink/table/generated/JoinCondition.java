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

package org.apache.flink.table.generated;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.table.dataformat.BaseRow;

/**
 * Interface for code generated condition function for [[org.apache.calcite.rel.core.Join]].
 */
public interface JoinCondition extends RichFunction {

	/**
	 * @return true if the join condition stays true for the joined row (in1, in2)
	 */
	boolean apply(BaseRow in1, BaseRow in2);

}
