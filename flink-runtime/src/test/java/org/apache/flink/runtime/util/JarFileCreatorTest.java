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

package org.apache.flink.runtime.util;

import org.apache.flink.runtime.util.jartestprogram.FilterWithIndirection;
import org.apache.flink.runtime.util.jartestprogram.FilterWithLambda;
import org.apache.flink.runtime.util.jartestprogram.FilterWithMethodReference;
import org.apache.flink.runtime.util.jartestprogram.WordCountWithAnonymousClass;
import org.apache.flink.runtime.util.jartestprogram.WordCountWithExternalClass;
import org.apache.flink.runtime.util.jartestprogram.WordCountWithExternalClass2;
import org.apache.flink.runtime.util.jartestprogram.WordCountWithInnerClass;
import org.apache.flink.runtime.util.jartestprogram.AnonymousInStaticMethod;
import org.apache.flink.runtime.util.jartestprogram.AnonymousInNonStaticMethod;
import org.apache.flink.runtime.util.jartestprogram.AnonymousInNonStaticMethod2;
import org.apache.flink.runtime.util.jartestprogram.NestedAnonymousInnerClass;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.JarInputStream;
import java.util.zip.ZipEntry;

public class JarFileCreatorTest {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	//anonymous inner class in static method accessing a local variable in its closure.
	@Test
	public void TestAnonymousInnerClassTrick1() throws Exception {
		File out = tempFolder.newFile("jarcreatortest.jar");
		JarFileCreator jfc = new JarFileCreator(out);
		jfc.addClass(AnonymousInStaticMethod.class)
			.createJarFile();

		Set<String> ans = new HashSet<>();
		ans.add("org/apache/flink/runtime/util/jartestprogram/AnonymousInStaticMethod$1.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/AnonymousInStaticMethod$A.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/AnonymousInStaticMethod.class");

		Assert.assertTrue("Jar file for Anonymous Inner Class is not correct", validate(ans, out));

		Assert.assertTrue(out.delete());
	}

	//anonymous inner class in non static method accessing a local variable in its closure.
	@Test
	public void TestAnonymousInnerClassTrick2() throws Exception {
		File out = new File(tempFolder.getRoot(), "jarcreatortest.jar");
		JarFileCreator jfc = new JarFileCreator(out);
		jfc.addClass(AnonymousInNonStaticMethod.class)
			.createJarFile();

		Set<String> ans = new HashSet<>();
		ans.add("org/apache/flink/runtime/util/jartestprogram/AnonymousInNonStaticMethod$1.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/AnonymousInNonStaticMethod$A.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/AnonymousInNonStaticMethod.class");

		Assert.assertTrue("Jar file for Anonymous Inner Class is not correct", validate(ans, out));

		Assert.assertTrue(out.delete());
	}

	//anonymous inner class in non static method accessing a field of its enclosing class.
	@Test
	public void TestAnonymousInnerClassTrick3() throws Exception {
		File out = new File(tempFolder.getRoot(), "jarcreatortest.jar");
		JarFileCreator jfc = new JarFileCreator(out);
		jfc.addClass(AnonymousInNonStaticMethod2.class)
			.createJarFile();

		Set<String> ans = new HashSet<>();
		ans.add("org/apache/flink/runtime/util/jartestprogram/AnonymousInNonStaticMethod2$1.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/AnonymousInNonStaticMethod2$A.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/AnonymousInNonStaticMethod2.class");

		Assert.assertTrue("Jar file for Anonymous Inner Class is not correct", validate(ans, out));

		Assert.assertTrue(out.delete());
	}

	//anonymous inner class in an anonymous inner class accessing a field of the outermost enclosing class.
	@Test
	public void TestAnonymousInnerClassTrick4() throws Exception {
		File out = new File(tempFolder.getRoot(), "jarcreatortest.jar");
		JarFileCreator jfc = new JarFileCreator(out);
		jfc.addClass(NestedAnonymousInnerClass.class)
			.createJarFile();

		Set<String> ans = new HashSet<>();
		ans.add("org/apache/flink/runtime/util/jartestprogram/NestedAnonymousInnerClass.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/NestedAnonymousInnerClass$1$1.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/NestedAnonymousInnerClass$1.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/NestedAnonymousInnerClass$A.class");

		Assert.assertTrue("Jar file for Anonymous Inner Class is not correct", validate(ans, out));

		Assert.assertTrue(out.delete());
	}

	@Ignore // this is currently not supported (see FLINK-9520)
	@Test
	public void testFilterWithMethodReference() throws Exception {
		File out = new File(tempFolder.getRoot(), "jarcreatortest.jar");
		JarFileCreator jfc = new JarFileCreator(out);
		jfc.addClass(FilterWithMethodReference.class)
			.createJarFile();

		Set<String> ans = new HashSet<>();
		ans.add("org/apache/flink/runtime/util/jartestprogram/FilterWithMethodReference.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/WordFilter.class");

		Assert.assertTrue("Jar file for Java 8 method reference is not correct", validate(ans, out));
		Assert.assertTrue(out.delete());
	}

	@Test
	public void testFilterWithLambda() throws Exception{
		File out = new File(tempFolder.getRoot(), "jarcreatortest.jar");
		JarFileCreator jfc = new JarFileCreator(out);
		jfc.addClass(FilterWithLambda.class)
			.createJarFile();

		Set<String> ans = new HashSet<>();
		ans.add("org/apache/flink/runtime/util/jartestprogram/FilterWithLambda.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/WordFilter.class");

		Assert.assertTrue("Jar file for Java 8 lambda is not correct", validate(ans, out));
		Assert.assertTrue(out.delete());
	}

	@Test
	public void testFilterWithIndirection() throws Exception {
		File out = new File(tempFolder.getRoot(), "jarcreatortest.jar");
		JarFileCreator jfc = new JarFileCreator(out);
		jfc.addClass(FilterWithIndirection.class)
			.createJarFile();

		Set<String> ans = new HashSet<>();
		ans.add("org/apache/flink/runtime/util/jartestprogram/FilterWithIndirection.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/WordFilter.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/UtilFunctionWrapper$UtilFunction.class");

		Assert.assertTrue("Jar file for java 8 lambda is not correct", validate(ans, out));
		Assert.assertTrue(out.delete());
	}

	//----------------------------------------------------------------------------------------------
	//Word Count Example

	@Test
	public void TestExternalClass() throws IOException {
		File out = new File(tempFolder.getRoot(), "jarcreatortest.jar");
		JarFileCreator jfc = new JarFileCreator(out);
		jfc.addClass(WordCountWithExternalClass.class)
			.createJarFile();

		Set<String> ans = new HashSet<>();
		ans.add("org/apache/flink/runtime/util/jartestprogram/StaticData.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/WordCountWithExternalClass.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/ExternalTokenizer.class");

		Assert.assertTrue("Jar file for External Class is not correct", validate(ans, out));

		Assert.assertTrue(out.delete());
	}

	@Test
	public void TestInnerClass() throws IOException {
		File out = new File(tempFolder.getRoot(), "jarcreatortest.jar");
		JarFileCreator jfc = new JarFileCreator(out);
		jfc.addClass(WordCountWithInnerClass.class)
			.createJarFile();

		Set<String> ans = new HashSet<>();
		ans.add("org/apache/flink/runtime/util/jartestprogram/StaticData.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/WordCountWithInnerClass.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/WordCountWithInnerClass$Tokenizer.class");

		Assert.assertTrue("Jar file for Inner Class is not correct", validate(ans, out));

		Assert.assertTrue(out.delete());
	}

	@Test
	public void TestAnonymousClass() throws IOException {
		File out = new File(tempFolder.getRoot(), "jarcreatortest.jar");
		JarFileCreator jfc = new JarFileCreator(out);
		jfc.addClass(WordCountWithAnonymousClass.class)
			.createJarFile();

		Set<String> ans = new HashSet<>();
		ans.add("org/apache/flink/runtime/util/jartestprogram/StaticData.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/WordCountWithAnonymousClass.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/WordCountWithAnonymousClass$1.class");

		Assert.assertTrue("Jar file for Anonymous Class is not correct", validate(ans, out));

		Assert.assertTrue(out.delete());
	}

	@Test
	public void TestExtendIdentifier() throws IOException {
		File out = new File(tempFolder.getRoot(), "jarcreatortest.jar");
		JarFileCreator jfc = new JarFileCreator(out);
		jfc.addClass(WordCountWithExternalClass2.class)
			.createJarFile();

		Set<String> ans = new HashSet<>();
		ans.add("org/apache/flink/runtime/util/jartestprogram/StaticData.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/WordCountWithExternalClass2.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/ExternalTokenizer2.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/ExternalTokenizer.class");

		Assert.assertTrue("Jar file for Extend Identifier is not correct", validate(ans, out));

		Assert.assertTrue(out.delete());
	}

	@Test
	public void TestUDFPackage() throws IOException {
		File out = new File(tempFolder.getRoot(), "jarcreatortest.jar");
		JarFileCreator jfc = new JarFileCreator(out);
		jfc.addClass(WordCountWithInnerClass.class)
			.addPackage("org.apache.flink.util")
			.createJarFile();

		Set<String> ans = new HashSet<>();
		ans.add("org/apache/flink/runtime/util/jartestprogram/StaticData.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/WordCountWithInnerClass.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/WordCountWithInnerClass$Tokenizer.class");
		ans.add("org/apache/flink/util/Collector.class");

		Assert.assertTrue("Jar file for UDF package is not correct", validate(ans, out));

		Assert.assertTrue(out.delete());
	}

	private boolean validate(Set<String> expected, File out) throws IOException {
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

