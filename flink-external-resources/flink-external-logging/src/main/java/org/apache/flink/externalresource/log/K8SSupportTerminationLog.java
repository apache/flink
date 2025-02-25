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

package org.apache.flink.externalresource.log;

import org.apache.flink.core.execution.TerminationLog;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

public class K8SSupportTerminationLog implements TerminationLog {

    protected static final Logger LOG = LoggerFactory.getLogger(K8SSupportTerminationLog.class);

    private static final int terminationLogLengthLimit = 4096;

    /**
     * flink: this is done so that any config failures can be bubbled up to controller. Refer to <a
     * href="https://kubernetes.io/docs/tasks/debug/debug-application/determine-reason-pod-failure/#customizing-the-termination-message>Kubernetes
     * doc</a>
     *
     * @param error
     */
    @Override
    public void writeTerminationLog(Throwable error, String errorCode) {
        LOG.info("Writing the error message to /dev/termination-log");
        String terminationLogFileName =
                System.getenv().getOrDefault("FLINK_TERMINATION_LOG", "/dev/termination-log");
        File terminationLog = new File(terminationLogFileName);
        try {
            ErrorRecord errorRecord = new ErrorRecord();
            errorRecord.setComponent("flink");
            errorRecord.setErrorCode(errorCode);
            errorRecord.setMessage(getStackTrace(error));

            ObjectMapper objectMapper = new ObjectMapper();
            terminationLog.getParentFile().mkdirs();
            if (!terminationLog.exists()) {
                terminationLog.createNewFile();
            }
            String errorMessage = objectMapper.writeValueAsString(errorRecord);
            if (errorMessage.length() > terminationLogLengthLimit) {
                int overflow = errorMessage.length() - terminationLogLengthLimit;
                errorRecord.truncateMessage(errorRecord.getMessage().length() - overflow);
                errorMessage = objectMapper.writeValueAsString(errorRecord);
            }
            Files.write(
                    terminationLog.toPath(),
                    errorMessage.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.WRITE);
        } catch (Exception e) {
            LOG.error("Error while writing to the termination log", e);
        }
    }

    private String getStackTrace(Throwable error) throws IOException {
        String errorMessage = "";
        try (StringWriter stringWriter = new StringWriter();
                PrintWriter writer = new PrintWriter(stringWriter, true)) {
            error.printStackTrace(writer);
            errorMessage = stringWriter.toString();
        }
        return errorMessage;
    }
}
