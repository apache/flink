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

package org.apache.flink.tools.ci.licensecheck;

//CHECKSTYLE.OFF: regexp|imports
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//CHECKSTYLE.ON: regexp|imports

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility class checking for proper NOTICE files based on the maven build output.
 */
public class NoticeFileChecker {

	private static final Logger LOG = LoggerFactory.getLogger(NoticeFileChecker.class);

	private static final List<String> MODULES_SKIPPING_DEPLOYMENT = loadFromResources("modules-skipping-deployment.modulelist");

	private static final List<String> MODULES_DEFINING_EXCESS_DEPENDENCIES = loadFromResources("modules-defining-excess-dependencies.modulelist");

	// pattern for maven shade plugin
	private static final Pattern SHADE_NEXT_MODULE_PATTERN = Pattern.compile(".*:shade \\((shade-flink|default)\\) @ ([^ _]+)(_[0-9.]+)? --.*");
	private static final Pattern SHADE_INCLUDE_MODULE_PATTERN = Pattern.compile(".*Including ([^:]+):([^:]+):jar:([^ ]+) in the shaded jar");

	// pattern for maven-dependency-plugin copyied dependencies
	private static final Pattern DEPENDENCY_COPY_NEXT_MODULE_PATTERN = Pattern.compile(".*maven-dependency-plugin:[^:]+:copy \\([^)]+\\) @ ([^ _]+)(_[0-9.]+)? --.*");
	private static final Pattern DEPENDENCY_COPY_INCLUDE_MODULE_PATTERN = Pattern.compile(".*Configured Artifact: ([^:]+):([^:]+):([^:]+):jar.*");

	// Examples:
	// "- org.apache.htrace:htrace-core:3.1.0-incubating"
	// or
	// "This project bundles "net.jcip:jcip-annotations:1.0".
	private static final Pattern NOTICE_DEPENDENCY_PATTERN = Pattern.compile("- ([^ :]+):([^:]+):([^ ]+)($| )|.*bundles \"([^:]+):([^:]+):([^\"]+)\".*");

	int run(File buildResult, Path root) throws IOException {
		int severeIssueCount = 0;
		// parse included dependencies from build output
		Multimap<String, IncludedDependency> modulesWithBundledDependencies = parseModulesFromBuildResult(buildResult);
		LOG.info("Extracted " + modulesWithBundledDependencies.asMap().keySet().size() + " modules with a total of " + modulesWithBundledDependencies.values().size() + " dependencies");

		// find modules producing a shaded-jar
		List<Path> noticeFiles = findNoticeFiles(root);
		LOG.info("Found {} NOTICE files to check", noticeFiles.size());

		// check that all required NOTICE files exists
		severeIssueCount += ensureRequiredNoticeFiles(modulesWithBundledDependencies, noticeFiles);

		// check each NOTICE file
		for (Path noticeFile: noticeFiles) {
			severeIssueCount += checkNoticeFile(modulesWithBundledDependencies, noticeFile);
		}

		return severeIssueCount;
	}

	private static int ensureRequiredNoticeFiles(Multimap<String, IncludedDependency> modulesWithShadedDependencies, List<Path> noticeFiles) {
		int severeIssueCount = 0;
		Set<String> shadingModules = new HashSet<>(modulesWithShadedDependencies.keys());
		shadingModules.removeAll(noticeFiles.stream().map(NoticeFileChecker::getModuleFromNoticeFile).collect(Collectors.toList()));
		for (String moduleWithoutNotice : shadingModules) {
			if (!MODULES_SKIPPING_DEPLOYMENT.contains(moduleWithoutNotice)) {
				LOG.warn("Module {} is missing a NOTICE file. It has shaded dependencies: {}", moduleWithoutNotice, modulesWithShadedDependencies.get(moduleWithoutNotice));
				severeIssueCount++;
			}
		}
		return severeIssueCount;
	}

	private static String getModuleFromNoticeFile(Path noticeFile) {
		Path moduleDirectory = noticeFile.getParent() // META-INF
			.getParent() // resources
			.getParent() // main
			.getParent() // src
			.getParent(); // <-- module name
		return moduleDirectory.getFileName().toString();
	}

	private static int checkNoticeFile(Multimap<String, IncludedDependency> modulesWithShadedDependencies, Path noticeFile) throws IOException {
		int severeIssueCount = 0;
		String moduleName = getModuleFromNoticeFile(noticeFile);

		// 1st line contains module name
		List<String> noticeContents = Files.readAllLines(noticeFile);

		if (noticeContents.isEmpty()) {
			LOG.error("Notice file empty {}", noticeFile);
			severeIssueCount++;
		}

		// first line must be the module name.
		if (!noticeContents.get(0).equals(moduleName)) {
			LOG.warn("Expected first file of notice file to start with module name. moduleName={}, firstLine={}", moduleName, noticeContents.get(0));
		}

		// collect all declared dependencies from NOTICE file
		Set<IncludedDependency> declaredDependencies = new HashSet<>();
		for (String line : noticeContents) {
			Matcher noticeDependencyMatcher = NOTICE_DEPENDENCY_PATTERN.matcher(line);
			if (noticeDependencyMatcher.find()) {
				String groupId = noticeDependencyMatcher.group(1);
				String artifactId = noticeDependencyMatcher.group(2);
				String version = noticeDependencyMatcher.group(3);
				if (groupId == null && artifactId == null && version == null) { // "bundles" case
					groupId = noticeDependencyMatcher.group(5);
					artifactId = noticeDependencyMatcher.group(6);
					version = noticeDependencyMatcher.group(7);
				}
				IncludedDependency toAdd = IncludedDependency.create(groupId, artifactId, version);
				if (!declaredDependencies.add(toAdd)) {
					LOG.error("Dependency {} has been declared twice in module {}", toAdd, moduleName);
					severeIssueCount++;
				}
			}
		}
		// print all dependencies missing from NOTICE file
		Collection<IncludedDependency> expectedDependencies = modulesWithShadedDependencies.get(moduleName);
		for (IncludedDependency expectedDependency : expectedDependencies) {
			if (!declaredDependencies.contains(expectedDependency)) {
				LOG.error("Could not find dependency {} in NOTICE file {}", expectedDependency, noticeFile);
				severeIssueCount++;
			}
		}

		boolean moduleDefinesExcessDependencies = MODULES_DEFINING_EXCESS_DEPENDENCIES.contains(moduleName);

		// print all dependencies defined in NOTICE file, which were not expected
		for (IncludedDependency declaredDependency : declaredDependencies) {
			if (!expectedDependencies.contains(declaredDependency)) {
				if (moduleDefinesExcessDependencies) {
					LOG.debug(
						"Dependency {} is mentioned in NOTICE file {}, but was not mentioned by the build output as a bundled dependency",
						declaredDependency,
						noticeFile);
				} else {
					LOG.warn(
						"Dependency {} is mentioned in NOTICE file {}, but is not expected there",
						declaredDependency,
						noticeFile);
				}

			}
		}

		return severeIssueCount;
	}

	private static List<Path> findNoticeFiles(Path root) throws IOException {
		return Files.walk(root)
			.filter(file -> {
				int nameCount = file.getNameCount();
				return file.getName(nameCount - 3).toString().equals("resources")
					&& file.getName(nameCount - 2).toString().equals("META-INF")
					&& file.getName(nameCount - 1).toString().equals("NOTICE");
			})
			.collect(Collectors.toList());
	}

	private static Multimap<String, IncludedDependency> parseModulesFromBuildResult(File buildResult) throws IOException {
		Multimap<String, IncludedDependency> result = ArrayListMultimap.create();

		try (Stream<String> lines = Files.lines(buildResult.toPath())) {
			//String line;
			String currentShadeModule = null;
			String currentDependencyCopyModule = null;
			for (String line : (Iterable<String>) lines::iterator) {
				Matcher nextShadeModuleMatcher = SHADE_NEXT_MODULE_PATTERN.matcher(line);
				if (nextShadeModuleMatcher.find()) {
					currentShadeModule = nextShadeModuleMatcher.group(2);
				}

				Matcher nextDependencyCopyModuleMatcher = DEPENDENCY_COPY_NEXT_MODULE_PATTERN.matcher(line);
				if (nextDependencyCopyModuleMatcher.find()) {
					currentDependencyCopyModule = nextDependencyCopyModuleMatcher.group(1);
				}

				if (currentShadeModule != null) {
					Matcher includeMatcher = SHADE_INCLUDE_MODULE_PATTERN.matcher(line);
					if (includeMatcher.find()) {
						String groupId = includeMatcher.group(1);
						String artifactId = includeMatcher.group(2);
						String version = includeMatcher.group(3);
						if (!"org.apache.flink".equals(groupId)) {
							result.put(currentShadeModule, IncludedDependency.create(groupId, artifactId, version));
						}
					}
				}

				if (currentDependencyCopyModule != null) {
					Matcher copyMatcher = DEPENDENCY_COPY_INCLUDE_MODULE_PATTERN.matcher(line);
					if (copyMatcher.find()) {
						String groupId = copyMatcher.group(1);
						String artifactId = copyMatcher.group(2);
						String version = copyMatcher.group(3);
						if (!"org.apache.flink".equals(groupId)) {
							result.put(currentDependencyCopyModule, IncludedDependency.create(groupId, artifactId, version));
						}
					}
				}
				if (line.contains("Replacing original artifact with shaded artifact")) {
					currentShadeModule = null;
				}
				if (line.contains("Copying")) {
					currentDependencyCopyModule = null;
				}
			}
		}
		return result;
	}

	private static List<String> loadFromResources(String fileName) {
		try {
			Path resource = Paths.get(NoticeFileChecker.class.getResource("/" + fileName).toURI());
			List<String> result = Files
				.readAllLines(resource)
				.stream()
				.filter(line -> !line.startsWith("#") && !line.isEmpty())
				.collect(Collectors.toList());
			LOG.debug("Loaded {} items from resource {}", result.size(), fileName);
			return result;
		} catch (Throwable e) {
			// wrap anything in a RuntimeException to be callable from the static initializer
			throw new RuntimeException("Error while loading resource", e);
		}
	}

	private static final class IncludedDependency {

		private final String groupId;
		private final String artifactId;
		private final String version;

		private IncludedDependency(String groupId, String artifactId, String version) {
			this.groupId = Preconditions.checkNotNull(groupId);
			this.artifactId = Preconditions.checkNotNull(artifactId);
			this.version = Preconditions.checkNotNull(version);
		}

		public static IncludedDependency create(String groupId, String artifactId, String version) {
			return new IncludedDependency(groupId, artifactId, version);
		}

		@Override
		public String toString() {
			return groupId + ":" + artifactId + ":" + version;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			IncludedDependency that = (IncludedDependency) o;

			if (!groupId.equals(that.groupId)) {
				return false;
			}
			if (!artifactId.equals(that.artifactId)) {
				return false;
			}
			return version.equals(that.version);
		}

		@Override
		public int hashCode() {
			int result = groupId.hashCode();
			result = 31 * result + artifactId.hashCode();
			result = 31 * result + version.hashCode();
			return result;
		}
	}
}
