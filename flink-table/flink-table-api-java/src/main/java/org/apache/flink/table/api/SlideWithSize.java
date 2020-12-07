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
import org.apache.flink.table.expressions.ExpressionParser;

/**
 * Partially specified sliding window. The size of the window either as time or row-count interval.
 */
@PublicEvolving
public final class SlideWithSize {

	/** The size of the window either as time or row-count interval. */
	private final Expression size;

	SlideWithSize(Expression size) {
		this.size = ApiExpressionUtils.unwrapFromApi(size);
	}

	/**
	 * Specifies the window's slide as time or row-count interval.
	 *
	 * <p>The slide determines the interval in which windows are started. Hence, sliding windows can
	 * overlap if the slide is smaller than the size of the window.
	 *
	 * <p>For example, you could have windows of size 15 minutes that slide by 3 minutes. With this
	 * 15 minutes worth of elements are grouped every 3 minutes and each row contributes to 5
	 * windows.
	 *
	 * @param slide the slide of the window either as time or row-count interval.
	 * @return a sliding window
	 * @deprecated use {@link #every(Expression)}
	 */
	@Deprecated
	public SlideWithSizeAndSlide every(String slide) {
		return every(ExpressionParser.parseExpression(slide));
	}

	/**
	 * Specifies the window's slide as time or row-count interval.
	 *
	 * <p>The slide determines the interval in which windows are started. Hence, sliding windows can
	 * overlap if the slide is smaller than the size of the window.
	 *
	 * <p>For example, you could have windows of size 15 minutes that slide by 3 minutes. With this
	 * 15 minutes worth of elements are grouped every 3 minutes and each row contributes to 5
	 * windows.
	 *
	 * @param slide the slide of the window either as time or row-count interval.
	 * @return a sliding window
	 */
	public SlideWithSizeAndSlide every(Expression slide) {
		return new SlideWithSizeAndSlide(size, slide);
	}
}
