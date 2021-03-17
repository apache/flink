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

package org.apache.flink.runtime.operators.testutils;

import org.apache.flink.runtime.operators.testutils.types.StringPair;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;

public class UniformStringPairGenerator implements MutableObjectIterator<StringPair> {

    private final int numKeys;
    private final int numVals;

    private int keyCnt = 0;
    private int valCnt = 0;
    private boolean repeatKey;

    public UniformStringPairGenerator(int numKeys, int numVals, boolean repeatKey) {
        this.numKeys = numKeys;
        this.numVals = numVals;
        this.repeatKey = repeatKey;
    }

    @Override
    public StringPair next(StringPair target) throws IOException {
        if (!repeatKey) {
            if (valCnt >= numVals) {
                return null;
            }

            target.setKey(Integer.toString(keyCnt++));
            target.setValue(Integer.toBinaryString(valCnt));

            if (keyCnt == numKeys) {
                keyCnt = 0;
                valCnt++;
            }
        } else {
            if (keyCnt >= numKeys) {
                return null;
            }

            target.setKey(Integer.toString(keyCnt));
            target.setValue(Integer.toBinaryString(valCnt++));

            if (valCnt == numVals) {
                valCnt = 0;
                keyCnt++;
            }
        }

        return target;
    }

    @Override
    public StringPair next() throws IOException {
        return next(new StringPair());
    }
}
