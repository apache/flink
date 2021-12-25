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

package org.apache.flink.connector.base.source.reader.mocks;

/** The state of the {@link MockSplitReader} reflecting the progress. */
class MockSplitState {
    private int recordIndex;
    private final int endRecordIndex;

    public MockSplitState(int recordIndex, int endRecordIndex) {
        this.recordIndex = recordIndex;
        this.endRecordIndex = endRecordIndex;
    }

    public void setRecordIndex(int recordIndex) {
        this.recordIndex = recordIndex;
    }

    public int getEndRecordIndex() {
        return endRecordIndex;
    }

    public int getRecordIndex() {
        return recordIndex;
    }

    public int getPendingRecords() {
        return endRecordIndex - recordIndex - 1;
    }
}
