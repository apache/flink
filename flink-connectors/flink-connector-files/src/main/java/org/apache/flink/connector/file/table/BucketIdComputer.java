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

package org.apache.flink.connector.file.table;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;

/** A computer to compute a bucket id for the record as well as the bucket file's prefix. */
@Internal
public interface BucketIdComputer<T> extends Serializable {

    int getBucketId(T in);

    String getBucketFilePrefix(int bucketId);

    int DEFAULT_BUCKET_ID = 0;

    /**
     * A default implementation for {@link BucketIdComputer}. All the records will be assigned the
     * same bucket, and prefix of the bucket file is an empty string.
     */
    class DefaultBucketIdComputer<T> implements BucketIdComputer<T> {

        @Override
        public int getBucketId(T in) {
            return DEFAULT_BUCKET_ID;
        }

        @Override
        public String getBucketFilePrefix(int bucketId) {
            return "";
        }
    }
}
