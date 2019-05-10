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

package org.apache.flink.table.util;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Test cases for {@link JCACompilationUtil}.
 */
public class JCACompilationUtilTest {

	@Test
	public void testGetClassFilePath() {
		String srcPath = Paths.get(JCACompilationUtil.CODE_ROOT_FILE.getAbsolutePath(), "org",  "apache",  "flink", "Test.java").toString();
		String expClsPath = Paths.get(JCACompilationUtil.CODE_ROOT_FILE.getAbsolutePath(), "org",  "apache",  "flink", "Test.class").toString();
		String actualClsPath = JCACompilationUtil.getInstance().getClassFilePath(srcPath);
		Assert.assertEquals(expClsPath, actualClsPath);
	}

	@Test
	public void testGetClassName() {
		String srcPath = Paths.get(JCACompilationUtil.CODE_ROOT_FILE.getAbsolutePath(), "org",  "apache",  "flink", "Test.java").toString();
		String expClass = JCACompilationUtil.getInstance().getClassName(srcPath);
		Assert.assertEquals("org.apache.flink.Test", expClass);
	}

	@Test
	public void testGetSourceFilePath() {
		final String className = "org.apache.flink.Test";
		String expPath = Paths.get(JCACompilationUtil.CODE_ROOT_FILE.getAbsolutePath(), "org", "apache", "flink", "Test.java").toString();
		String actualPath = JCACompilationUtil.getInstance().getSourceFilePath(className);
		Assert.assertEquals(expPath, actualPath);
	}

	@Test
	public void testWriteSource() throws IOException {
		final String className = "org.apache.flink.Test";
		final String code = "package org.apache.flink;\n" +
			"public class Test {\n" +
			"\tpublic static void main(String args[]) {\n" +
			"\t\tSystem.out.println(\"Hello World\");\n" +
			"\t}\n" +
			"}\n";

		String srcPath = JCACompilationUtil.getInstance().writeSource(className, code);

		Assert.assertTrue(new File(srcPath).exists());
		String contents = new String(Files.readAllBytes(Paths.get(srcPath)));
		Assert.assertEquals(code, contents);

		// clean up
		for (File file : JCACompilationUtil.CODE_ROOT_FILE.listFiles()) {
			FileUtils.deleteDirectory(file);
		}
	}

	@Test
	public void testWriteCompileLoad() throws IOException {
		// step 1 - write source file
		final String className = "org.apache.flink.Test";
		final String code = "package org.apache.flink;\n" +
			"public class Test {\n" +
			"\tpublic static void main(String args[]) {\n" +
			"\t\tSystem.out.println(\"Hello World\");\n" +
			"\t}\n" +
			"}\n";

		String srcPath = JCACompilationUtil.getInstance().writeSource(className, code);

		// step 2 - compile the source file
		JCACompilationUtil.getInstance().compile(srcPath);
		String clsPath = JCACompilationUtil.getInstance().getClassFilePath(srcPath);
		Assert.assertTrue(new File(clsPath).exists());

		// step 3 - load the class from cache
		Class<?> clazz = JCACompilationUtil.getInstance().findClassInCache(className, code);
		Assert.assertNull(clazz);

		// step 4 - load class directly
		clazz = JCACompilationUtil.getInstance().loadClass(srcPath);
		Assert.assertNotNull(clazz);
		Assert.assertEquals(className, clazz.getCanonicalName());

		// step 5 - clean up
		for (File file : JCACompilationUtil.CODE_ROOT_FILE.listFiles()) {
			FileUtils.deleteDirectory(file);
		}
	}

	@Test
	public void testCompileInBatch() {
		List<String> classNames = new ArrayList<>();
		List<String> sourceNames = new ArrayList<>();

		classNames.add("org.apache.flink.Test1");
		sourceNames.add("package org.apache.flink;\n" +
			"public class Test1 {\n" +
			"\tpublic static void main(String args[]) {\n" +
			"\t\tSystem.out.println(\"Hello World\");\n" +
			"\t}\n" +
			"}\n");

		classNames.add("org.apache.flink.Test2");
		sourceNames.add("package org.apache.flink;\n" +
			"public class Test2 {\n" +
			"\tpublic static void main(String args[]) {\n" +
			"\t\tSystem.out.println(\"Hello World\");\n" +
			"\t}\n" +
			"}\n");

		classNames.add("org.apache.flink.Test3");
		sourceNames.add("package org.apache.flink;\n" +
			"public class Test3 {\n" +
			"\tpublic static void main(String args[]) {\n" +
			"\t\tSystem.out.println(\"Hello World\");\n" +
			"\t}\n" +
			"}\n");

		JCACompilationUtil.getInstance().compileSourcesInBatch(classNames, sourceNames);

		Class<?> clazz0 = JCACompilationUtil.getInstance().findClassInCache(classNames.get(0), sourceNames.get(0));
		Assert.assertNotNull(clazz0);
		Assert.assertEquals("org.apache.flink.Test1", clazz0.getCanonicalName());

		Class<?> clazz1 = JCACompilationUtil.getInstance().findClassInCache(classNames.get(1), sourceNames.get(1));
		Assert.assertNotNull(clazz1);
		Assert.assertEquals("org.apache.flink.Test2", clazz1.getCanonicalName());

		Class<?> clazz2 = JCACompilationUtil.getInstance().findClassInCache(classNames.get(2), sourceNames.get(2));
		Assert.assertNotNull(clazz2);
		Assert.assertEquals("org.apache.flink.Test3", clazz2.getCanonicalName());

		String className = "org.apache.flink.Test4";
		String sourceName = "package org.apache.flink;\n" +
			"public class Test4 {\n" +
			"\tpublic static void main(String args[]) {\n" +
			"\t\tSystem.out.println(\"Hello World\");\n" +
			"\t}\n" +
			"}\n";

		Class<?> clazz3 = JCACompilationUtil.getInstance().getCodeClass(className, sourceName);
		Assert.assertNotNull(clazz3);
		Assert.assertEquals("org.apache.flink.Test4", clazz3.getCanonicalName());
	}
}
