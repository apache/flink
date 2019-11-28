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

package org.apache.flink.table.runtime.generated;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.table.dataformat.BaseRow;

import javax.annotation.Nullable;

/**
 * The {@link WatermarkGenerator} is used to generate watermark based the input elements.
 */
public abstract class WatermarkGenerator extends AbstractRichFunction {

	private static final long serialVersionUID = 1L;

	/**
	 * Returns the watermark for the current row or null if no watermark should be generated.
	 *
	 * @param row The current row.
	 * @return The watermark for this row or null if no watermark should be generated.
	 */
	@Nullable
	public abstract Long currentWatermark(BaseRow row) throws Exception;
}
