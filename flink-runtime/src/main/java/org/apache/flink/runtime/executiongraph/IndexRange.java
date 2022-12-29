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

package org.apache.flink.runtime.executiongraph;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;

/** This class represents the range of index. The range is inclusive. */
public class IndexRange implements Serializable {

    private final int startIndex;
    private final int endIndex;

    public IndexRange(int startIndex, int endIndex) {
        checkArgument(startIndex >= 0);
        checkArgument(endIndex >= startIndex);

        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }

    public int getStartIndex() {
        return startIndex;
    }

    public int getEndIndex() {
        return endIndex;
    }

    public int size() {
        return endIndex - startIndex + 1;
    }

    @Override
    public String toString() {
        return String.format("[%d, %d]", startIndex, endIndex);
    }

    @Override
    public int hashCode() {
        return this.startIndex * 31 + this.endIndex;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj != null && obj.getClass() == getClass()) {
            IndexRange that = (IndexRange) obj;
            return that.startIndex == this.startIndex && that.endIndex == this.endIndex;
        } else {
            return false;
        }
    }
}
