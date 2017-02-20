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

package org.apache.flink.runtime.codegeneration;

import freemarker.template.TemplateException;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.codehaus.janino.JavaSourceClassLoader;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

public class SorterFactory {

	private static SorterFactory sorterFactory;

	private ClassLoader classLoader;
	private TemplateManager templateManager;

	public SorterFactory() throws IOException {
		this.templateManager = TemplateManager.getInstance();
		this.classLoader = new JavaSourceClassLoader(
			SorterFactory.class.getClassLoader(),
			new File[] { new File(templateManager.GENERATING_PATH) },
			"UTF-8"
		);
	}

	public static SorterFactory getInstance() throws IOException {
		if( sorterFactory == null ){
			return new SorterFactory();
		}

		return sorterFactory;
	}


	public InMemorySorter createSorter(TypeSerializer serializer, TypeComparator comparator, List<MemorySegment> memory ) throws IOException, TemplateException, ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {

		String className = this.templateManager.getGeneratedCode(new SorterTemplateModel(comparator));

		Constructor sorterConstructor = classLoader.loadClass(className).getConstructor(
			TypeSerializer.class, TypeComparator.class, List.class
		);

		Object generatedSorter = sorterConstructor.newInstance(serializer, comparator, memory);

		System.out.println(">> " + generatedSorter.toString());

		return (InMemorySorter)generatedSorter;
	}
}
