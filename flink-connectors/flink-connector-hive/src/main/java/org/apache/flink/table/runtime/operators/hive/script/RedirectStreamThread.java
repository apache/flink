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

package org.apache.flink.table.runtime.operators.hive.script;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A thread just read bytes from one input stream and write to one output stream.
 *
 * <p>Note: This thread won't close inputStream or outputStream if encounter any exception.
 */
public class RedirectStreamThread extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedirectStreamThread.class);

    private final InputStream inputStream;
    private final OutputStream outputStream;

    public RedirectStreamThread(
            InputStream inputStream, OutputStream outputStream, String threadName) {
        super(threadName);
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        setDaemon(true);
    }

    @Override
    public void run() {
        byte[] buf = new byte[1024];
        int length;
        try {
            while ((length = inputStream.read(buf)) > 0) {
                outputStream.write(buf, 0, length);
            }
        } catch (IOException e) {
            LOGGER.warn("Fail to redirect input stream to output stream.", e);
        }
    }
}
