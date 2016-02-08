/**
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
package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * An operator that can sort a stream based on timestamps. Arriving elements will be put into
 * buckets based on their timestamp. Sorting and emission of sorted elements happens once
 * the watermark passes the end of a bucket.
 *
 * @param <T> The type of the elements on which this operator works.
 */
@Internal
public class BucketStreamSortOperator<T> extends AbstractStreamOperator<T> implements OneInputStreamOperator<T, T> {
	private static final long serialVersionUID = 1L;

	private long granularity;

	private transient Map<Long, List<StreamRecord<T>>> buckets;

	/**
	 * Creates a new sorting operator that creates buckets with the given interval.
	 *
	 * @param interval The size (in time) of one bucket.
	 */
	public BucketStreamSortOperator(long interval) {
		this.granularity = interval;
	}

	@Override
	public void open() throws Exception {
		super.open();
		buckets = new HashMap<>();

	}

	@Override
	@SuppressWarnings("unchecked")
	public void processElement(StreamRecord<T> record) throws Exception {
		long bucketId = record.getTimestamp() - (record.getTimestamp() % granularity);
		List<StreamRecord<T>> bucket = buckets.get(bucketId);
		if (bucket == null) {
			bucket = new ArrayList<>();
			buckets.put(bucketId, bucket);
		}
		bucket.add(record);
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		long maxBucketId = mark.getTimestamp() - (mark.getTimestamp() % granularity);
		Set<Long> toRemove = new HashSet<>();
		for (Map.Entry<Long, List<StreamRecord<T>>> bucket: buckets.entrySet()) {
			if (bucket.getKey() < maxBucketId) {
				Collections.sort(bucket.getValue(), new Comparator<StreamRecord<T>>() {
					@Override
					public int compare(StreamRecord<T> o1, StreamRecord<T> o2) {
						return (int) (o1.getTimestamp() - o2.getTimestamp());
					}
				});
				for (StreamRecord<T> r: bucket.getValue()) {
					output.collect(r);
				}
				toRemove.add(bucket.getKey());
			}
		}

		for (Long l: toRemove) {
			buckets.remove(l);
		}

		output.emitWatermark(mark);
	}

}
