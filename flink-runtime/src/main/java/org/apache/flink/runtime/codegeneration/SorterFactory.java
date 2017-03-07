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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.sort.NormalizedKeySorter;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.SimpleCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * {@link SorterFactory} is a singleton class that provides functionalities to create the most suitable sorter
 * for underlying data based on {@link TypeComparator}
 */
public class SorterFactory {
	// ------------------------------------------------------------------------
	//                                   Constants
	// ------------------------------------------------------------------------
	private static final Logger LOG = LoggerFactory.getLogger(SorterFactory.class);

	// ------------------------------------------------------------------------
	//                                   Singleton Attribute
	// ------------------------------------------------------------------------
	private static SorterFactory sorterFactory;

	// ------------------------------------------------------------------------
	//                                   Attributes
	// ------------------------------------------------------------------------
	private SimpleCompiler classComplier;
	private TemplateManager templateManager;

	/**
	 * Constructor
	 * @throws IOException
	 */
	public SorterFactory() throws IOException {
		this.templateManager = TemplateManager.getInstance();
		this.classComplier = new SimpleCompiler();
	}

	/**
	 * A method to get a singleton instance
	 * or create one if it has been created yet
	 * @return
	 * @throws IOException
	 */
	public static SorterFactory getInstance() throws IOException {
		if( sorterFactory == null ){
			synchronized(SorterFactory.class){
				sorterFactory = new SorterFactory();
			}
		}

		return sorterFactory;
	}


	/**
	 * Create a sorter for the given type comparator and
	 * assign serializer, comparator and memory to the sorter
	 * @param serializer
	 * @param comparator
	 * @param memory
	 * @return
	 * @throws IOException
	 * @throws TemplateException
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * @throws NoSuchMethodException
	 * @throws InvocationTargetException
	 */
	public InMemorySorter createSorter(ExecutionConfig config, TypeSerializer serializer, TypeComparator comparator, List<MemorySegment> memory ) throws IOException, TemplateException, ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException, CompileException {

		InMemorySorter sorter = null;

		if(config.isCodeGenerationForSorterEnabled()){
			String className = this.templateManager.getGeneratedCode(new SorterTemplateModel(comparator));

			this.classComplier.cookFile(this.templateManager.getPathToGeneratedCode(className));

			Constructor sorterConstructor = this.classComplier.getClassLoader().loadClass(className).getConstructor(
				TypeSerializer.class, TypeComparator.class, List.class
			);

			sorter = (InMemorySorter)sorterConstructor.newInstance(serializer, comparator, memory);

			if(LOG.isDebugEnabled()){
				LOG.debug("Creating a custom sorter : " + sorter.toString());
			}
		} else {
			sorter = new NormalizedKeySorter(serializer, comparator, memory);
		}


		return sorter;
	}
}
