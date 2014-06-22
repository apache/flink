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

import org.apache.flink.runtime.util.jartestprogram.WordCountWithAnonymousClass;
import org.apache.flink.runtime.util.jartestprogram.WordCountWithExternalClass;
import org.apache.flink.runtime.util.jartestprogram.WordCountWithExternalClass2;
import org.apache.flink.runtime.util.jartestprogram.WordCountWithInnerClass;
import org.junit.Assert;
import org.junit.Test;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.JarInputStream;
import java.util.zip.ZipEntry;


public class JarFileCreatorTest {

	@Test
	public void TestExternalClass() throws IOException {
		File out = new File("/tmp/jarcreatortest1.jar");
		JarFileCreator jfc = new JarFileCreator(out);
		jfc.addClass(WordCountWithExternalClass.class).createJarFile();

		Set<String> ans = new HashSet<String>();
		ans.add("org/apache/flink/runtime/util/jartestprogram/StaticData.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/WordCountWithExternalClass.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/ExternalTokenizer.class");
		JarInputStream jis = new JarInputStream(new FileInputStream(out));
		ZipEntry ze;
		int count = 3;
		while ((ze = jis.getNextEntry()) != null) {
			count--;
			ans.remove(ze.getName());
		}
		Assert.assertTrue("Jar file for External Class is not correct", count == 0 && ans.size() == 0);

		out.delete();
	}

	@Test
	public void TestInnerClass() throws IOException {
		File out = new File("/tmp/jarcreatortest2.jar");
		JarFileCreator jfc = new JarFileCreator(out);
		jfc.addClass(WordCountWithInnerClass.class).createJarFile();

		Set<String> ans = new HashSet<String>();
		ans.add("org/apache/flink/runtime/util/jartestprogram/StaticData.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/WordCountWithInnerClass.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/WordCountWithInnerClass$Tokenizer.class");
		JarInputStream jis = new JarInputStream(new FileInputStream(out));
		ZipEntry ze;
		int count = 3;
		while ((ze = jis.getNextEntry()) != null) {
			count--;
			ans.remove(ze.getName());
		}
		Assert.assertTrue("Jar file for Inner Class is not correct", count == 0 && ans.size() == 0);

		out.delete();
	}

	@Test
	public void TestAnonymousClass() throws IOException {
		File out = new File("/tmp/jarcreatortest3.jar");
		JarFileCreator jfc = new JarFileCreator(out);
		jfc.addClass(WordCountWithAnonymousClass.class).createJarFile();

		Set<String> ans = new HashSet<String>();
		ans.add("org/apache/flink/runtime/util/jartestprogram/StaticData.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/WordCountWithAnonymousClass.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/WordCountWithAnonymousClass$1.class");
		JarInputStream jis = new JarInputStream(new FileInputStream(out));
		ZipEntry ze;
		int count = 3;
		while ((ze = jis.getNextEntry()) != null) {
			count--;
			ans.remove(ze.getName());
		}
		Assert.assertTrue("Jar file for Anonymous Class is not correct", count == 0 && ans.size() == 0);

		out.delete();
	}

	@Test
	public void TestExtendIdentifier() throws IOException {
		File out = new File("/tmp/jarcreatortest4.jar");
		JarFileCreator jfc = new JarFileCreator(out);
		jfc.addClass(WordCountWithExternalClass2.class).createJarFile();

		Set<String> ans = new HashSet<String>();
		ans.add("org/apache/flink/runtime/util/jartestprogram/StaticData.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/WordCountWithExternalClass2.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/ExternalTokenizer2.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/ExternalTokenizer.class");
		JarInputStream jis = new JarInputStream(new FileInputStream(out));
		ZipEntry ze;
		int count = 4;
		while ((ze = jis.getNextEntry()) != null) {
			count--;
			ans.remove(ze.getName());
		}
		Assert.assertTrue("Jar file for Extend Identifier is not correct", count == 0 && ans.size() == 0);

		out.delete();
	}

	@Test
	public void TestUDFPackage() throws IOException {
		File out = new File("/tmp/jarcreatortest5.jar");
		JarFileCreator jfc = new JarFileCreator(out);
		jfc.addClass(WordCountWithInnerClass.class)
			.addPackage("org.apache.flink.util").createJarFile();

		Set<String> ans = new HashSet<String>();
		ans.add("org/apache/flink/runtime/util/jartestprogram/StaticData.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/WordCountWithInnerClass.class");
		ans.add("org/apache/flink/runtime/util/jartestprogram/WordCountWithInnerClass$Tokenizer.class");
		ans.add("org/apache/flink/util/Collector.class");
		JarInputStream jis = new JarInputStream(new FileInputStream(out));
		ZipEntry ze;
		int count = 4;
		while ((ze = jis.getNextEntry()) != null) {
			count--;
			ans.remove(ze.getName());
		}
		Assert.assertTrue("Jar file for UDF package is not correct", count == 0 && ans.size() == 0);

		out.delete();
	}

}

