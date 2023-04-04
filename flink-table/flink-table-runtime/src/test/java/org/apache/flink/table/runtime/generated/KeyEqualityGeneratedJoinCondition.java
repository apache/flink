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

package org.apache.flink.table.runtime.generated;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.RowData;

/**
 * {@link GeneratedJoinCondition} that returns a {@link JoinCondition} comparing rows keys using
 * provided {@link KeySelector}s. It is only used for easy testing.
 */
public class KeyEqualityGeneratedJoinCondition extends GeneratedJoinCondition {

    private final KeySelector<RowData, RowData> keySelector1;
    private final KeySelector<RowData, RowData> keySelector2;

    public KeyEqualityGeneratedJoinCondition(
            KeySelector<RowData, RowData> keySelector1,
            KeySelector<RowData, RowData> keySelector2) {
        super("", "", new Object[0]);
        this.keySelector1 = keySelector1;
        this.keySelector2 = keySelector2;
    }

    @Override
    public JoinCondition newInstance(ClassLoader classLoader) {
        return new JoinByKeyEquationCondition();
    }

    /** Join condition for comparing keys of RowData. */
    private class JoinByKeyEquationCondition extends AbstractRichFunction implements JoinCondition {

        @Override
        public boolean apply(RowData in1, RowData in2) {
            try {
                return keySelector1.getKey(in1).equals(keySelector2.getKey(in2));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
