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

package org.apache.flink.runtime.messages.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.testutils.ClassLoaderUtils;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URLClassLoader;
import java.util.Optional;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link DeclineCheckpoint} message. */
class DeclineCheckpointTest {

    @TempDir private java.nio.file.Path tempDir;

    @Test
    void testDeclineCheckpointWithUserExceptionCanBeDeserializedWithoutUserClass()
            throws Exception {
        final String className = "UserException";
        final URLClassLoader userClassLoader =
                ClassLoaderUtils.compileAndLoadJava(
                        TempDirUtils.newFolder(tempDir),
                        className + ".java",
                        String.format(
                                "public class %s extends RuntimeException { public %s() {super(\"UserMessage\");} }",
                                className, className));

        Throwable userException =
                (Throwable) Class.forName(className, false, userClassLoader).newInstance();

        CheckpointException checkpointException =
                new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED, userException);

        final byte[] serializedCheckpointMessage =
                InstantiationUtil.serializeObject(
                        new DeclineCheckpoint(
                                new JobID(), createExecutionAttemptId(), 1, checkpointException));
        final DeclineCheckpoint deserializedCheckpointMessage =
                InstantiationUtil.deserializeObject(
                        serializedCheckpointMessage, ClassLoader.getSystemClassLoader());

        Throwable throwable =
                deserializedCheckpointMessage.getSerializedCheckpointException().unwrap();
        assertThat(throwable).isInstanceOf(CheckpointException.class);
        Optional<Throwable> throwableWithMessage =
                ExceptionUtils.findThrowableWithMessage(throwable, userException.getMessage());
        assertThat(throwableWithMessage)
                .isPresent()
                .hasValueSatisfying(
                        throwable1 ->
                                assertThat(throwable1)
                                        .hasMessage(
                                                String.format(
                                                        "%s: %s",
                                                        userException.getClass().getName(),
                                                        userException.getMessage())));
    }
}
