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

package org.apache.flink.table.connector.source.abilities;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.source.ScanTableSource;

/**
 * Enables to push down a limit (the expected maximum number of produced records) into a {@link ScanTableSource}.
 *
 * <p>It might be beneficial to perform the limiting as early as possible in order to be close to the
 * actual data generation.
 *
 * <p>A source can perform the limiting on a best-effort basis. During runtime, it must not guarantee
 * that the number of emitted records is less than or equal to the limit.
 *
 * <p>Regardless if this interface is implemented or not, a limit is also applied in a subsequent operation
 * after the source.
 */
@PublicEvolving
public interface SupportsLimitPushDown {

	/**
	 * Provides the expected maximum number of produced records for limiting on a best-effort basis.
	 */
	void applyLimit(long limit);
}
