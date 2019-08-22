/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.windowing.clickeventcount.records;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;

import java.util.Date;
import java.util.Objects;

/**
 * A small wrapper class for windowed page counts.
 *
 */
public class ClickEventStatistics {

	//using java.util.Date for better readability in Flink Cluster Playground
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy hh:mm:ss:SSS")
	private Date windowStart;
	//using java.util.Date for better readability in Flink Cluster Playground
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy hh:mm:ss:SSS")
	private Date windowEnd;
	private String page;
	private long count;

	public ClickEventStatistics() {
	}

	public ClickEventStatistics(
			final Date windowStart,
			final Date windowEnd,
			final String page,
			final long count) {
		this.windowStart = windowStart;
		this.windowEnd = windowEnd;
		this.page = page;
		this.count = count;
	}

	public Date getWindowStart() {
		return windowStart;
	}

	public void setWindowStart(final Date windowStart) {
		this.windowStart = windowStart;
	}

	public Date getWindowEnd() {
		return windowEnd;
	}

	public void setWindowEnd(final Date windowEnd) {
		this.windowEnd = windowEnd;
	}

	public String getPage() {
		return page;
	}

	public void setPage(final String page) {
		this.page = page;
	}

	public long getCount() {
		return count;
	}

	public void setCount(final long count) {
		this.count = count;
	}

	@Override
	public boolean equals(final Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		final ClickEventStatistics that = (ClickEventStatistics) o;
		return count == that.count &&
				Objects.equals(windowStart, that.windowStart) &&
				Objects.equals(windowEnd, that.windowEnd) &&
				Objects.equals(page, that.page);
	}

	@Override
	public int hashCode() {
		return Objects.hash(windowStart, windowEnd, page, count);
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("ClickEventStatistics{");
		sb.append("windowStart=").append(windowStart);
		sb.append(", windowEnd=").append(windowEnd);
		sb.append(", page='").append(page).append('\'');
		sb.append(", count=").append(count);
		sb.append('}');
		return sb.toString();
	}
}
