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

package org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;

/**
 * A {@link BucketAssigner} that does not perform any bucketing of files. All files are written to
 * the base path.
 */
@PublicEvolving
public class BasePathBucketAssigner<T> implements BucketAssigner<T, String> {

    private static final long serialVersionUID = -6033643155550226022L;

    @Override
    public String getBucketId(T element, BucketAssigner.Context context) {
        return "";
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        // in the future this could be optimized as it is the empty string.
        return SimpleVersionedStringSerializer.INSTANCE;
    }

    @Override
    public String toString() {
        return "BasePathBucketAssigner";
    }
}
