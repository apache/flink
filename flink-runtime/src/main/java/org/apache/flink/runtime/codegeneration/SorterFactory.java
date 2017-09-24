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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.operators.sort.FixedLengthRecordSorter;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.sort.NormalizedKeySorter;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import freemarker.template.Version;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.SimpleCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;

/**
 * {@link SorterFactory} is a singleton class that provides functionalities to create the most suitable sorter
 * for underlying data based on {@link TypeComparator}.
 * Note: the generated code can be inspected by configuring Janino to write the code that is being compiled
 * to a file, see http://janino-compiler.github.io/janino/#debugging
 */
public class SorterFactory {
	// ------------------------------------------------------------------------
	//                                   Constants
	// ------------------------------------------------------------------------
	private static final Logger LOG = LoggerFactory.getLogger(SorterFactory.class);

	/** Fixed length records with a length below this threshold will be in-place sorted, if possible. */
	private static final int THRESHOLD_FOR_IN_PLACE_SORTING = 32;

	// ------------------------------------------------------------------------
	//                                   Singleton Attribute
	// ------------------------------------------------------------------------
	private static SorterFactory sorterFactory;

	// ------------------------------------------------------------------------
	//                                   Attributes
	// ------------------------------------------------------------------------
	private SimpleCompiler classCompiler;
	private HashMap<String, Constructor> constructorCache;
	private final Template template;

	/**
	 * This is only for testing. If an error occurs, we want to fail the test, instead of falling back
	 * to a non-generated sorter.
	 */
	public boolean forceCodeGeneration = false;

	/**
	 * Constructor.
	 */
	private SorterFactory() {
		this.classCompiler = new SimpleCompiler();
		this.classCompiler.setParentClassLoader(this.getClass().getClassLoader());
		this.constructorCache = new HashMap<>();
		Configuration templateConf;
		templateConf = new Configuration(new Version(2, 3, 26));
		templateConf.setClassForTemplateLoading(SorterFactory.class, "/templates");
		templateConf.setDefaultEncoding("UTF-8");
		templateConf.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
		try {
			template = templateConf.getTemplate(SorterTemplateModel.TEMPLATE_NAME);
		} catch (IOException e) {
			throw new RuntimeException("Couldn't read sorter template.", e);
		}
	}

	/**
	 * A method to get a singleton instance
	 * or create one if it hasn't been created yet.
	 * @return
	 */
	public static synchronized SorterFactory getInstance() {
		if (sorterFactory == null){
			sorterFactory = new SorterFactory();
		}

		return sorterFactory;
	}


	/**
	 * Create a sorter for the given type comparator and
	 * assign serializer, comparator and memory to the sorter.
	 * @param serializer
	 * @param comparator
	 * @param memory
	 * @return
	 */
	public <T> InMemorySorter<T> createSorter(ExecutionConfig config, TypeSerializer<T> serializer,
											TypeComparator<T> comparator, List<MemorySegment> memory) {
		if (config.isCodeGenerationForSortersEnabled()) {
			try {
				return createCodegenSorter(serializer, comparator, memory);
			} catch (IOException | TemplateException | ClassNotFoundException | IllegalAccessException |
					InstantiationException | NoSuchMethodException | InvocationTargetException | CompileException e) {

				String msg = "Serializer: " + serializer +
						"[" + serializer + "], comparator: [" + comparator + "], exception: " + e.toString();
				if (!forceCodeGeneration) {
					LOG.warn("An error occurred while trying to create a code-generated sorter. " +
							"Using non-codegen sorter instead. " + msg);
					return createNonCodegenSorter(serializer, comparator, memory);
				} else {
					throw new RuntimeException("An error occurred while trying to create a code-generated sorter. " +
							"Failing the job, because forceCodeGeneration is true. " + msg);
				}
			}
		} else {
			return createNonCodegenSorter(serializer, comparator, memory);
		}
	}

	private <T> InMemorySorter<T> createCodegenSorter(TypeSerializer<T> serializer, TypeComparator<T> comparator,
													List<MemorySegment> memory)
			throws IOException, TemplateException, ClassNotFoundException, IllegalAccessException,
			InstantiationException, NoSuchMethodException, InvocationTargetException, CompileException {
		SorterTemplateModel sorterModel = new SorterTemplateModel(comparator);

		Constructor sorterConstructor;

		synchronized (this){
			if (constructorCache.containsKey(sorterModel.getSorterName())) {
				sorterConstructor = constructorCache.get(sorterModel.getSorterName());
			} else {
				String sorterName = sorterModel.getSorterName();

				StringWriter generatedCodeWriter = new StringWriter();
				template.process(sorterModel.getTemplateVariables(), generatedCodeWriter);

				this.classCompiler.cook(generatedCodeWriter.toString());

				sorterConstructor = this.classCompiler.getClassLoader().loadClass(sorterName).getConstructor(
						TypeSerializer.class, TypeComparator.class, List.class
				);

				constructorCache.put(sorterName, sorterConstructor);
			}
		}

		@SuppressWarnings("unchecked")
		InMemorySorter<T> sorter = (InMemorySorter<T>) sorterConstructor.newInstance(serializer, comparator, memory);

		if (LOG.isInfoEnabled()){
			LOG.info("Using a custom sorter : " + sorter.toString());
		}

		return sorter;
	}

	private <T> InMemorySorter<T> createNonCodegenSorter(TypeSerializer<T> serializer, TypeComparator<T> comparator,
														List<MemorySegment> memory) {
		InMemorySorter<T> sorter;
		// instantiate a fix-length in-place sorter, if possible, otherwise the out-of-place sorter
		if (comparator.supportsSerializationWithKeyNormalization() &&
				serializer.getLength() > 0 && serializer.getLength() <= THRESHOLD_FOR_IN_PLACE_SORTING &&
				comparator.isNormalizedKeyPrefixOnly(comparator.getNormalizeKeyLen())) {
			// Note about the last part of the condition:
			// FixedLengthRecordSorter doesn't do an additional check after the bytewise comparison, so
			// we cannot choose that if the normalized key doesn't always determine the order.
			// (cf. the part of NormalizedKeySorter.compare after the if)
			sorter = new FixedLengthRecordSorter<>(serializer, comparator.duplicate(), memory);
		} else {
			sorter = new NormalizedKeySorter<>(serializer, comparator.duplicate(), memory);
		}
		return sorter;
	}
}
