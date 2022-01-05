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

package org.apache.flink.runtime.rest.messages;

import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Test for (un)marshalling of the {@link ThreadDumpInfo}. */
public class ThreadDumpInfoTest extends RestResponseMarshallingTestBase<ThreadDumpInfo> {

    @Override
    protected Class<ThreadDumpInfo> getTestResponseClass() {
        return ThreadDumpInfo.class;
    }

    @Override
    protected ThreadDumpInfo getTestResponseInstance() throws Exception {
        final Collection<ThreadDumpInfo.ThreadInfo> threadInfos =
                Arrays.asList(
                        ThreadDumpInfo.ThreadInfo.create("foobar", "barfoo"),
                        ThreadDumpInfo.ThreadInfo.create("bar", "foo"));

        return ThreadDumpInfo.create(threadInfos);
    }

    @Override
    protected void assertOriginalEqualsToUnmarshalled(
            ThreadDumpInfo expected, ThreadDumpInfo actual) {
        assertThat(
                actual.getThreadInfos(), containsInAnyOrder(expected.getThreadInfos().toArray()));
    }

    @Test
    public void testComparedWithDefaultJDKImplemetation() {
        ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
        ThreadInfo threadInfo =
                threadMxBean.getThreadInfo(Thread.currentThread().getId(), Integer.MAX_VALUE);

        // JDK11 has increased the output info of threadInfo.daemon and threadInfo.priority compared
        // to JDK8, hence only compare the output of stacktrace content for compatibility.
        String[] threadInfoLines = threadInfo.toString().split("\n");
        String[] expected = Arrays.copyOfRange(threadInfoLines, 1, threadInfoLines.length);

        String stringifyThreadInfo = ThreadDumpInfo.stringifyThreadInfo(threadInfo, 8);
        String[] stringifyThreadInfoLines = stringifyThreadInfo.split("\n");
        String[] stringified =
                Arrays.copyOfRange(stringifyThreadInfoLines, 1, stringifyThreadInfoLines.length);

        assertArrayEquals(expected, stringified);
    }

    @Test
    public void testStacktraceDepthLimitation() {
        ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
        ThreadInfo threadInfo =
                threadMxBean.getThreadInfo(Thread.currentThread().getId(), Integer.MAX_VALUE);

        int expectedStacktraceDepth = threadInfo.getStackTrace().length;

        String stringifiedInfo = ThreadDumpInfo.stringifyThreadInfo(threadInfo, Integer.MAX_VALUE);
        assertEquals(expectedStacktraceDepth, getOutputDepth(stringifiedInfo));

        String stringifiedInfoExceedMaxDepth =
                ThreadDumpInfo.stringifyThreadInfo(threadInfo, expectedStacktraceDepth - 1);
        assertEquals(expectedStacktraceDepth - 1, getOutputDepth(stringifiedInfoExceedMaxDepth));
        assertTrue(stringifiedInfoExceedMaxDepth.contains("\t..."));
    }

    private long getOutputDepth(String stringifiedInfo) {
        return Arrays.stream(stringifiedInfo.split("\n")).filter(x -> x.contains("\tat ")).count();
    }
}
