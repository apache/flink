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

package org.apache.flink.tools.ci.utils.notice;

import org.apache.flink.tools.ci.utils.shared.Dependency;

import java.util.Collection;
import java.util.Collections;

/** Represents the parsed contents of a NOTICE file. */
public class NoticeContents {
    private final String noticeModuleName;
    private final Collection<Dependency> declaredDependencies;
    private final Collection<String> misorderedLines;

    public NoticeContents(String noticeModuleName, Collection<Dependency> declaredDependencies) {
        this(noticeModuleName, declaredDependencies, Collections.emptyList());
    }

    public NoticeContents(
            String noticeModuleName,
            Collection<Dependency> declaredDependencies,
            Collection<String> misorderedLines) {
        this.noticeModuleName = noticeModuleName;
        this.declaredDependencies = declaredDependencies;
        this.misorderedLines = misorderedLines;
    }

    public String getNoticeModuleName() {
        return noticeModuleName;
    }

    public Collection<Dependency> getDeclaredDependencies() {
        return declaredDependencies;
    }

    /** Dependency lines that put the version before the classifier. */
    public Collection<String> getMisorderedLines() {
        return misorderedLines;
    }
}
