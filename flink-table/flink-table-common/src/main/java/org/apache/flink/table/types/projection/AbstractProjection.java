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

package org.apache.flink.table.types.projection;

import java.util.Arrays;

abstract class AbstractProjection implements Projection {

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Projection)) {
            return false;
        }
        Projection other = (Projection) o;
        if (!this.isNested() && !other.isNested()) {
            return Arrays.equals(this.toTopLevelIndexes(), other.toTopLevelIndexes());
        }
        return Arrays.deepEquals(this.toNestedIndexes(), other.toNestedIndexes());
    }

    @Override
    public int hashCode() {
        if (isNested()) {
            return Arrays.deepHashCode(toNestedIndexes());
        }
        return Arrays.hashCode(toTopLevelIndexes());
    }

    @Override
    public String toString() {
        if (isNested()) {
            return "nested projection = " + Arrays.deepToString(toNestedIndexes());
        }
        return "top level projection = " + Arrays.toString(toTopLevelIndexes());
    }
}
