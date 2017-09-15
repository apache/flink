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

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Map;

/**
 * {@link TemplateManager} is a singleton class that provides template rendering functionalities for code generation.
 * Such functionalities are caching, writing generated code to a file.
 */
public class TemplateManager {
	// ------------------------------------------------------------------------
	//                                   Constants
	// ------------------------------------------------------------------------

	public static final String TEMPLATE_ENCODING  = "UTF-8";

	private static final Logger LOG = LoggerFactory.getLogger(TemplateManager.class);

	// ------------------------------------------------------------------------
	//                                   Singleton Attribute
	// ------------------------------------------------------------------------

	private static TemplateManager templateManager;

	// ------------------------------------------------------------------------
	//                                   Attributes
	// ------------------------------------------------------------------------

	private final Template template;

	/**
	 * Constructor.
	 */
	private TemplateManager() {
		Configuration templateConf;
		templateConf = new Configuration();
		templateConf.setClassForTemplateLoading(TemplateManager.class, "/templates");
		templateConf.setDefaultEncoding(TEMPLATE_ENCODING);
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
	 * @return The singleton instance.
	 */
	public static synchronized TemplateManager getInstance() {
		if (templateManager == null){
			templateManager = new TemplateManager();
		}

		return templateManager;
	}


	/**
	 * Render sorter template with generated code provided by SorterTemplateModel.
	 * @param model
	 * @return the generated code
	 * @throws IOException
	 * @throws TemplateException
	 */
	synchronized String getGeneratedCode(SorterTemplateModel model) throws IOException, TemplateException {

		Writer output = new StringWriter();

		Map templateVariables = model.getTemplateVariables();
		template.process(templateVariables, output);

		output.close();

		return output.toString();
	}
}
