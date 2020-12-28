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

package org.apache.flink.runtime.webmonitor.history;

import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;

import java.io.IOException;
import java.util.Collection;

/**
 * Interface for all classes that want to participate in the archiving of job-related json
 * responses.
 */
public interface JsonArchivist {

    /**
     * Returns a {@link Collection} of {@link ArchivedJson}s containing JSON responses and their
     * respective REST URL for a given job.
     *
     * <p>The collection should contain one entry for every response that could be generated for the
     * given job, for example one entry for each task. The REST URLs should be unique and must not
     * contain placeholders.
     *
     * @param graph AccessExecutionGraph for which the responses should be generated
     * @return Collection containing an ArchivedJson for every response that could be generated for
     *     the given job
     * @throws IOException thrown if the JSON generation fails
     */
    Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException;
}
