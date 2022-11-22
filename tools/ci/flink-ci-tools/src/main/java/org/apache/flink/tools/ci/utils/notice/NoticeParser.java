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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.tools.ci.utils.shared.Dependency;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Parsing utils for NOTICE files. */
public class NoticeParser {

    // "- org.apache.htrace:htrace-core:3.1.0-incubating"
    private static final Pattern NOTICE_DEPENDENCY_PATTERN =
            Pattern.compile(
                    "- "
                            + "(?<groupId>[^ ]*?):"
                            + "(?<artifactId>[^ ]*?):"
                            + "(?:(?<classifier>[^ ]*?):)?"
                            + "(?<version>[^ ]*?)"
                            + "($| )");
    // "This project bundles "net.jcip:jcip-annotations:1.0".
    private static final Pattern NOTICE_BUNDLES_DEPENDENCY_PATTERN =
            Pattern.compile(
                    ".*bundles \""
                            + "(?<groupId>[^ ]*?):"
                            + "(?<artifactId>[^ ]*?):"
                            + "(?:(?<classifier>[^ ]*?):)?"
                            + "(?<version>[^ ]*?)"
                            + "\".*");

    public static Optional<NoticeContents> parseNoticeFile(Path noticeFile) throws IOException {
        // 1st line contains module name
        final List<String> noticeContents = Files.readAllLines(noticeFile);

        return parseNoticeFile(noticeContents);
    }

    @VisibleForTesting
    static Optional<NoticeContents> parseNoticeFile(List<String> noticeContents) {
        if (noticeContents.isEmpty()) {
            return Optional.empty();
        }

        final String noticeModuleName = noticeContents.get(0);

        Collection<Dependency> declaredDependencies = new ArrayList<>();
        for (String line : noticeContents) {
            Optional<Dependency> dependency = tryParsing(NOTICE_DEPENDENCY_PATTERN, line);
            if (!dependency.isPresent()) {
                dependency = tryParsing(NOTICE_BUNDLES_DEPENDENCY_PATTERN, line);
            }
            dependency.ifPresent(declaredDependencies::add);
        }

        return Optional.of(new NoticeContents(noticeModuleName, declaredDependencies));
    }

    private static Optional<Dependency> tryParsing(Pattern pattern, String line) {
        Matcher matcher = pattern.matcher(line);
        if (matcher.find()) {
            String groupId = matcher.group("groupId");
            String artifactId = matcher.group("artifactId");
            String version = matcher.group("version");
            String classifier = matcher.group("classifier");
            return Optional.of(Dependency.create(groupId, artifactId, version, classifier));
        }
        return Optional.empty();
    }
}
