/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.elasticsearch;

import org.apache.flink.annotation.Internal;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Implementation of a {@link RequestIndexer} that buffers {@link ActionRequest ActionRequests}
 * before re-sending them to the Elasticsearch cluster upon request.
 */
@Internal
@NotThreadSafe
class BufferingNoOpRequestIndexer implements RequestIndexer {

	private ConcurrentLinkedQueue<ActionRequest> bufferedRequests;

	BufferingNoOpRequestIndexer() {
		this.bufferedRequests = new ConcurrentLinkedQueue<ActionRequest>();
	}

	@Override
	public void add(DeleteRequest... deleteRequests) {
		Collections.addAll(bufferedRequests, deleteRequests);
	}

	@Override
	public void add(IndexRequest... indexRequests) {
		Collections.addAll(bufferedRequests, indexRequests);
	}

	@Override
	public void add(UpdateRequest... updateRequests) {
		Collections.addAll(bufferedRequests, updateRequests);
	}

	void processBufferedRequests(RequestIndexer actualIndexer) {
		for (ActionRequest request : bufferedRequests) {
			if (request instanceof IndexRequest) {
				actualIndexer.add((IndexRequest) request);
			} else if (request instanceof DeleteRequest) {
				actualIndexer.add((DeleteRequest) request);
			} else if (request instanceof UpdateRequest) {
				actualIndexer.add((UpdateRequest) request);
			}
		}

		bufferedRequests.clear();
	}
}
