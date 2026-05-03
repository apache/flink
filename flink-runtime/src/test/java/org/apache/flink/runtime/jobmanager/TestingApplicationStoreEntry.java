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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.runtime.application.AbstractApplication;
import org.apache.flink.runtime.blob.PermanentBlobService;

import javax.annotation.Nullable;

import java.util.Collection;

/** {@link ApplicationStoreEntry} implementation for testing purposes. */
public class TestingApplicationStoreEntry implements ApplicationStoreEntry {

    private final ApplicationID applicationId;
    private final String name;
    @Nullable private final AbstractApplication application;

    public TestingApplicationStoreEntry(ApplicationID applicationId, String name) {
        this.applicationId = applicationId;
        this.name = name;
        this.application = null;
    }

    public TestingApplicationStoreEntry(AbstractApplication application) {
        this.applicationId = application.getApplicationId();
        this.name = application.getName();
        this.application = application;
    }

    @Override
    public AbstractApplication getApplication(
            PermanentBlobService blobService,
            Collection<JobInfo> recoveredJobInfos,
            Collection<JobInfo> recoveredTerminalJobInfos) {
        if (application != null) {
            return application;
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public ApplicationID getApplicationId() {
        return applicationId;
    }

    @Override
    public String getName() {
        return name;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder for creating {@link TestingApplicationStoreEntry} instances. */
    public static class Builder {
        private ApplicationID applicationId = new ApplicationID();
        private String name = "TestingApplication";
        private AbstractApplication application;

        public Builder setApplicationId(ApplicationID applicationId) {
            this.applicationId = applicationId;
            return this;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setApplication(AbstractApplication application) {
            this.application = application;
            return this;
        }

        public TestingApplicationStoreEntry build() {
            if (application != null) {
                return new TestingApplicationStoreEntry(application);
            }
            return new TestingApplicationStoreEntry(applicationId, name);
        }
    }
}
