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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;

/**
 * Listener about the status of {@link Bucket}.
 */
@Internal
public interface BucketLifeCycleListener<IN, BucketID> extends Serializable {

	/**
	 * Notifies a new bucket has been created.
	 *
	 * @param bucket The newly created bucket.
	 */
	void bucketCreated(Bucket<IN, BucketID> bucket);

	/**
	 * Notifies a bucket become inactive. A bucket becomes inactive after all
	 * the records received so far have been committed.
	 *
	 * @param bucket The bucket getting inactive.
	 */
	void bucketInactive(Bucket<IN, BucketID> bucket);

}
