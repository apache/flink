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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.Expression;

/**
 * Sliding window on time with alias. Fully specifies a window.
 */
@PublicEvolving
public final class SlideWithSizeAndSlideOnTimeWithAlias extends GroupWindow {

	private final Expression size;
	private final Expression slide;

	SlideWithSizeAndSlideOnTimeWithAlias(
			Expression alias,
			Expression timeField,
			Expression size,
			Expression slide) {
		super(alias, timeField);
		this.size = ApiExpressionUtils.unwrapFromApi(size);
		this.slide = ApiExpressionUtils.unwrapFromApi(slide);
	}

	public Expression getSize() {
		return size;
	}

	public Expression getSlide() {
		return slide;
	}
}
