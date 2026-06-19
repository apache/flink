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
package org.apache.flink.api.common.typeutils.base.array;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.base.BooleanComparator;

import static java.lang.Math.min;

@Internal
public class BooleanPrimitiveArrayComparator
        extends PrimitiveArrayComparator<boolean[], BooleanComparator> {
    public BooleanPrimitiveArrayComparator(boolean ascending) {
        super(ascending, new BooleanComparator(ascending));
    }

    @Override
    public int hash(boolean[] record) {
        int result = 0;
        for (boolean field : record) {
            result += field ? 1231 : 1237;
        }
        return result;
    }

    @Override
    public int compare(boolean[] first, boolean[] second) {
        for (int x = 0; x < min(first.length, second.length); x++) {
            int cmp = (second[x] == first[x] ? 0 : (first[x] ? 1 : -1));
            if (cmp != 0) {
                return ascending ? cmp : -cmp;
            }
        }
        int cmp = first.length - second.length;
        return ascending ? cmp : -cmp;
    }

    @Override
    public TypeComparator<boolean[]> duplicate() {
        BooleanPrimitiveArrayComparator dupe = new BooleanPrimitiveArrayComparator(this.ascending);
        dupe.setReference(this.reference);
        return dupe;
    }
}
