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

package org.apache.flink.core.fs;

import java.io.IOException;

/**
 * {@link RecoverableFsDataOutputStream} with fixed implementation of {@link #closeForCommit()} that
 * is based on using {@link #persist()} to ensure durability and creates the {@link
 * org.apache.flink.core.fs.RecoverableFsDataOutputStream.Committer} from the corresponding {@link
 * org.apache.flink.core.fs.RecoverableWriter.ResumeRecoverable}.
 *
 * @param <RESUME_RECOVERABLE> return type of #persist()
 */
public abstract class CommitterFromPersistRecoverableFsDataOutputStream<
                RESUME_RECOVERABLE extends RecoverableWriter.ResumeRecoverable>
        extends RecoverableFsDataOutputStream {

    /** @see RecoverableFsDataOutputStream#persist() */
    @Override
    public abstract RESUME_RECOVERABLE persist() throws IOException;

    /**
     * @see RecoverableFsDataOutputStream#closeForCommit()
     * @param recoverable a resume recoverable to create the committer from. Typically the parameter
     *     is the return value of {@link #persist()}.
     * @return the committer created from recoverable.
     */
    protected abstract Committer createCommitterFromResumeRecoverable(
            RESUME_RECOVERABLE recoverable);

    /**
     * @see RecoverableFsDataOutputStream#closeForCommit()
     * @implNote Calls persist to ensure durability of the written data and creates a committer
     *     object from the return value of {@link #persist()}.
     */
    @Override
    public final Committer closeForCommit() throws IOException {
        Committer committer = createCommitterFromResumeRecoverable(persist());
        close();
        return committer;
    }
}
