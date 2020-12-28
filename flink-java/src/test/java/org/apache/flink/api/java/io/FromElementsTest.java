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

package org.apache.flink.api.java.io;

import org.apache.flink.api.java.ExecutionEnvironment;

import org.junit.Test;

/** Tests for {@link ExecutionEnvironment#fromElements}. */
public class FromElementsTest {

    @Test
    public void fromElementsWithBaseTypeTest1() {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.fromElements(
                ParentType.class, new SubType(1, "Java"), new ParentType(1, "hello"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void fromElementsWithBaseTypeTest2() {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.fromElements(
                SubType.class, new SubType(1, "Java"), new ParentType(1, "hello"));
    }

    private static class ParentType {
        int num;
        String string;

        public ParentType(int num, String string) {
            this.num = num;
            this.string = string;
        }
    }

    private static class SubType extends ParentType {
        public SubType(int num, String string) {
            super(num, string);
        }
    }
}
