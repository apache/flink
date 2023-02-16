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

package org.apache.flink.util.concurrent;

import org.slf4j.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.Arrays;
import java.util.stream.Collectors;

/** {@code ThreadUtils} collects helper methods in the context of threading. */
public class ThreadUtils {

    public static void errorLogThreadDump(Logger logger) {
        final ThreadInfo[] perThreadInfo =
                ManagementFactory.getThreadMXBean().dumpAllThreads(true, true);
        logger.error(
                "Thread dump: \n{}",
                Arrays.stream(perThreadInfo).map(Object::toString).collect(Collectors.joining()));
    }
}
