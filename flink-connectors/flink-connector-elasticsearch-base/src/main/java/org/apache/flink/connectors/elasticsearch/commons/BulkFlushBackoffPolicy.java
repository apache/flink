/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.elasticsearch.commons;


import java.io.Serializable;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class BulkFlushBackoffPolicy implements Serializable {

	private static final long serialVersionUID = -6022851996101826049L;

	// the default values follow the Elasticsearch default settings for BulkProcessor
	private FlushBackoffType backoffType = FlushBackoffType.EXPONENTIAL;
	private int maxRetryCount = 8;
	private long delayMillis = 50;

	public FlushBackoffType getBackoffType() {
		return backoffType;
	}

	public int getMaxRetryCount() {
		return maxRetryCount;
	}

	public long getDelayMillis() {
		return delayMillis;
	}

	public void setBackoffType(FlushBackoffType backoffType) {
		this.backoffType = checkNotNull(backoffType);
	}

	public void setMaxRetryCount(int maxRetryCount) {
		checkArgument(maxRetryCount >= 0);
		this.maxRetryCount = maxRetryCount;
	}

	public void setDelayMillis(long delayMillis) {
		checkArgument(delayMillis >= 0);
		this.delayMillis = delayMillis;
	}
}
