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

package org.apache.flink.runtime.operators.lifecycle.command;

/**
 * A command to be executed by a test operator. Upon execution, it must be acked by sending an
 * {@link org.apache.flink.runtime.operators.lifecycle.event.TestCommandAckEvent Ack} event.
 */
public interface TestCommand {
    TestCommand DELAY_SNAPSHOT =
            new TestCommand() {
                @Override
                public boolean isTerminal() {
                    return false;
                }

                @Override
                public String toString() {
                    return "DELAY_SNAPSHOT";
                }
            };
    TestCommand FAIL_SNAPSHOT =
            new TestCommand() {
                @Override
                public boolean isTerminal() {
                    return true;
                }

                @Override
                public String toString() {
                    return "FAIL_SNAPSHOT";
                }
            };
    TestCommand FAIL =
            new TestCommand() {
                @Override
                public boolean isTerminal() {
                    return true;
                }

                @Override
                public String toString() {
                    return "FAIL";
                }
            };
    TestCommand FINISH_SOURCES =
            new TestCommand() {
                @Override
                public boolean isTerminal() {
                    return true;
                }

                @Override
                public String toString() {
                    return "FINISH_SOURCES";
                }
            };

    boolean isTerminal();
}
