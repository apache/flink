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

package org.apache.flink.test.util.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

/**
 * A split enumerator where the first reader gets one split, others get nothing.
 *
 * <p>Useful for tests that need minimal data processing on a single subtask.
 */
@PublicEvolving
public class SingleSplitEnumerator extends TestSplitEnumerator<Void> {

    private boolean assigned = false;

    public SingleSplitEnumerator(SplitEnumeratorContext<TestSplit> context) {
        super(context, null);
    }

    @Override
    public void addReader(int subtaskId) {
        if (!assigned) {
            context.assignSplit(TestSplit.INSTANCE, subtaskId);
            assigned = true;
        }
        context.signalNoMoreSplits(subtaskId);
    }
}
