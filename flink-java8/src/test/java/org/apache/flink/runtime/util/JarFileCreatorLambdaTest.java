/**
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

package org.apache.flink.runtime.util;

import org.apache.flink.runtime.util.jartestprogram.FilterLambda1;
import org.apache.flink.runtime.util.jartestprogram.FilterLambda2;
import org.apache.flink.runtime.util.jartestprogram.FilterLambda3;
import org.apache.flink.runtime.util.jartestprogram.FilterLambda4;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.JarInputStream;
import java.util.zip.ZipEntry;

/**
 * Tests for the {@link JarFileCreator}.
 */
public class JarFileCreatorLambdaTest {
	@Test
	public void testFilterFunctionOnLambda1() throws Exception {
		File out = new File(System.getProperty("java.io.tmpdir"), "jarcreatortest.jar");
		JarFileCreator jfc = new JarFileCreator(out);
		jfc.addClass(FilterLambda1.class)
			.createJarFile();

		Set<String> ans = new HashSet<String>();
		ans.add("org/apache/flink/runtime/util/jartestprogram/FilterLambda1.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/WordFilter.class");

		Assert.assertTrue("Jar file for java 8 lambda is not correct", validate(ans, out));
		out.delete();
	}

	@Test
	public void testFilterFunctionOnLambda2() throws Exception{
		File out = new File(System.getProperty("java.io.tmpdir"), "jarcreatortest.jar");
		JarFileCreator jfc = new JarFileCreator(out);
		jfc.addClass(FilterLambda2.class)
			.createJarFile();

		Set<String> ans = new HashSet<String>();
		ans.add("org/apache/flink/runtime/util/jartestprogram/FilterLambda2.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/WordFilter.class");

		Assert.assertTrue("Jar file for java 8 lambda is not correct", validate(ans, out));
		out.delete();
	}

	@Test
	public void testFilterFunctionOnLambda3() throws Exception {
		File out = new File(System.getProperty("java.io.tmpdir"), "jarcreatortest.jar");
		JarFileCreator jfc = new JarFileCreator(out);
		jfc.addClass(FilterLambda3.class)
			.createJarFile();

		Set<String> ans = new HashSet<String>();
		ans.add("org/apache/flink/runtime/util/jartestprogram/FilterLambda3.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/WordFilter.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/UtilFunction.class");

		Assert.assertTrue("Jar file for java 8 lambda is not correct", validate(ans, out));
		out.delete();
	}

	@Test
	public void testFilterFunctionOnLambda4() throws Exception {
		File out = new File(System.getProperty("java.io.tmpdir"), "jarcreatortest.jar");
		JarFileCreator jfc = new JarFileCreator(out);
		jfc.addClass(FilterLambda4.class)
			.createJarFile();

		Set<String> ans = new HashSet<String>();
		ans.add("org/apache/flink/runtime/util/jartestprogram/FilterLambda4.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/WordFilter.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/UtilFunctionWrapper$UtilFunction.class");

		Assert.assertTrue("Jar file for java 8 lambda is not correct", validate(ans, out));
		out.delete();
	}

	public boolean validate(Set<String> expected, File out) throws Exception {
		int count = expected.size();
		try (JarInputStream jis = new JarInputStream(new FileInputStream(out))) {
			ZipEntry ze;
			while ((ze = jis.getNextEntry()) != null) {
				count--;
				expected.remove(ze.getName());
			}
		}
		return count == 0 && expected.size() == 0;
	}
}
