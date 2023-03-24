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

package org.apache.flink.changelog.fs;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

/** Implementation class for {@link StateChangeUploadScheduler} to test. */
class TestingBatchingUploadScheduler implements StateChangeUploadScheduler {
    private final Queue<UploadTask> tasks;
    private final StateChangeUploader uploader;

    public TestingBatchingUploadScheduler(StateChangeUploader uploader) {
        this.tasks = new LinkedList<>();
        this.uploader = uploader;
    }

    @Override
    public void upload(UploadTask uploadTask) throws IOException {
        tasks.add(uploadTask);
    }

    public void scheduleAll() throws IOException {
        if (tasks.size() > 0) {
            uploader.upload(tasks).complete();
        }
        tasks.clear();
    }

    @Override
    public void close() throws Exception {
        tasks.clear();
        uploader.close();
    }
}
