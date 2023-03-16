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

package org.apache.flink.connector.base.source.reader.splitreader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.source.reader.RecordEvaluator;

import java.util.List;

/**
 * A change to remove splits.
 *
 * <p>This type of {@link SplitsChange} is only used when the {@link RecordEvaluator} reports the
 * end of the stream.
 *
 * <p>The SplitsRemoval change between the enumerator and the reader may cause a data loss. See more
 * details at <a href="https://lists.apache.org/thread/7r4h7v5k281w9cnbfw9lb8tp56r30lwt">the
 * discussion e-mail</a>.
 *
 * @param <SplitT> the split type.
 */
@Internal
public class SplitsRemoval<SplitT> extends SplitsChange<SplitT> {
    public SplitsRemoval(List<SplitT> splits) {
        super(splits);
    }

    @Override
    public String toString() {
        return String.format("SplitsRemoval:[%s]", splits());
    }
}
