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

import org.apache.logging.log4j.core.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Checks the Jar files created by the build process.
 */
public class JarFileChecker {
	private static final Logger LOG = LoggerFactory.getLogger(JarFileChecker.class);

	public int run(Path path) throws IOException {
		List<Path> files = getBuildJars(path);
		LOG.info("Checking directory {} with a total of {} jar files.", path, files.size());

		int severeIssues = 0;
		for (Path file: files) {
			severeIssues += checkJar(file);
		}

		return severeIssues;
	}

	private int checkJar(Path file) throws IOException {
		boolean metaInfNoticeSeen = false;
		boolean metaInfLicenseSeen = false;
		int classFiles = 0;

		List<String> errors = new ArrayList<>();
		try (ZipInputStream zis = new ZipInputStream(new FileInputStream(file.toFile()))) {
			ZipEntry ze;
			while ((ze = zis.getNextEntry()) != null) {
				final String name = ze.getName();
				/**
				 * There must be a LICENSE file and NOTICE file in the META-INF directory.
				 * LICENSE or NOTICE files found outside of the META-INF directory are most likely shading mistakes (we are including the files from other dependencies, thus providing an invalid LICENSE file)
				 *
				 * In such a case, we recommend updating the shading exclusions, and adding the license file to META-INF/licenses.
				 */
				if (!name.contains("META-INF") // license files in META-INF are expected
					&& !name.contains(".class") // some class files contain LICENSE in their name
					&& !name.contains(".ftl") // a false positive in flink-python
					&& !name.contains("web/3rdpartylicenses.txt") // a false positive in flink-runtime-web
					&& !name.endsWith("/") // ignore directories, e.g. "license/"
					&& name.toUpperCase().contains("LICENSE")) {
					errors.add("Jar file " + file + " contains a LICENSE file in an unexpected location: " + name);
				}
				if (!name.contains("META-INF") && !name.contains(".class") && name.toUpperCase().contains("NOTICE")) {
					errors.add("Jar file " + file + " contains a NOTICE file in an unexpected location: " + name);
				}

				if (name.equals("META-INF/NOTICE")) {
					String noticeContents =  IOUtils.toString(new InputStreamReader(zis));
					if (!noticeContents.toLowerCase().contains("flink") || !noticeContents.contains("The Apache Software Foundation")) {
						errors.add("The notice file in " + file + ":META-INF/NOTICE does not contain the expected entries");
					}
					metaInfNoticeSeen = true;
				}
				if (name.equals("META-INF/LICENSE")) {
					String licenseContents =  IOUtils.toString(new InputStreamReader(zis));
					if (!licenseContents.contains("Apache License") || !licenseContents.contains("Version 2.0, January 2004")) {
						errors.add("The notice file in " + file + ":META-INF/LICENSE does not contain the expected entries");
					}
					metaInfLicenseSeen = true;
				}
				if (name.endsWith(".class")) {
					classFiles++;
				}
				zis.closeEntry();
			}
		}

		// Empty test jars (with no class files) are skipped
		if (classFiles == 0 && file.toString().endsWith("-tests.jar")) {
			return 0;
		}
		for (String error: errors) {
			LOG.warn(error);
		}
		int severeIssues = errors.size();

		if (!metaInfLicenseSeen) {
			LOG.warn("Missing META-INF/LICENSE in {}", file);
			severeIssues++;
		}

		if (!metaInfNoticeSeen) {
			LOG.warn("Missing META-INF/NOTICE in {}", file);
			severeIssues++;
		}
		return severeIssues;
	}

	private List<Path> getBuildJars(Path path) throws IOException {
		return Files.walk(path)
			.filter(file -> file.toString().endsWith(".jar"))
			.collect(Collectors.toList());
	}

	/**
	 * Main method for development purposes.
	 *
	 * <p>Generate a deploy-directory with:
	 * 	mvn clean deploy -DaltDeploymentRepository=snapshot-repo::default::file:/tmp/flink-deployment -DskipTests -Drat.skip
	 */
	public static void main(String[] args) throws IOException {
		if (args.length < 1) {
			System.out.println("Usage: JarFileChecker <pathFlinkDeployed>");
			System.exit(1);
		}
		JarFileChecker jarChecker = new JarFileChecker();
		if (jarChecker.run(Paths.get(args[0])) > 0) {
			LOG.warn("Found severe license issues");
			System.exit(1);
		}
		LOG.info("Jar check completed without severe issues.");
	}
}
