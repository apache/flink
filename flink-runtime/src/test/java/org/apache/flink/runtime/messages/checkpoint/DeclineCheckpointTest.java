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
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.testutils.ClassLoaderUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.URLClassLoader;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link DeclineCheckpoint} message. */
public class DeclineCheckpointTest extends TestLogger {

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @Test
    public void testDeclineCheckpointWithUserExceptionCanBeDeserializedWithoutUserClass()
            throws Exception {
        final String className = "UserException";
        final URLClassLoader userClassLoader =
                ClassLoaderUtils.compileAndLoadJava(
                        TEMPORARY_FOLDER.newFolder(),
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
                                new JobID(), new ExecutionAttemptID(), 1, checkpointException));
        final DeclineCheckpoint deserializedCheckpointMessage =
                InstantiationUtil.deserializeObject(
                        serializedCheckpointMessage, ClassLoader.getSystemClassLoader());

        Throwable throwable =
                deserializedCheckpointMessage.getSerializedCheckpointException().unwrap();
        assertThat(throwable, instanceOf(CheckpointException.class));
        Optional<Throwable> throwableWithMessage =
                ExceptionUtils.findThrowableWithMessage(throwable, userException.getMessage());
        assertTrue(throwableWithMessage.isPresent());
        assertThat(throwableWithMessage.get().getMessage(), equalTo(userException.getMessage()));
    }
}
