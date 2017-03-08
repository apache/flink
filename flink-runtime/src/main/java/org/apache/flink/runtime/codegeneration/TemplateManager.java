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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
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
	private final Configuration templateConf;
	private final Map<String,Boolean> generatedSorter;
	private final String dirForGeneratedCode;

	/**
	 * Constructor
	 * @throws IOException
	 */
	public TemplateManager() throws IOException {
		templateConf = new Configuration();
		String templatePath = TemplateManager.class.getClassLoader().getResource("templates").getPath();
		templateConf.setDirectoryForTemplateLoading(new File(templatePath));
		templateConf.setDefaultEncoding(TEMPLATE_ENCODING);
		templateConf.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

		generatedSorter = new HashMap<>();

		dirForGeneratedCode = prepareDirectoryGeneratedCode();
	}


	/**
	 * A method to get a singleton instance
	 * or create one if it has been created yet
	 * @return
	 * @throws IOException
	 */
	public static TemplateManager getInstance() throws IOException {
		if( templateManager == null ) {
			synchronized (TemplateManager.class){
				templateManager = new TemplateManager();
			}
		}

		return templateManager;
	}


	/**
	 * Render sorter template with generated code provided by SorterTemplateModel and write the content to a file
	 * and cache the result for later calls
	 * @param SorterTemplateModel object
	 * @return name of the generated sorter
	 * @throws IOException
	 * @throws TemplateException
	 */
	public String getGeneratedCode(SorterTemplateModel model) throws IOException, TemplateException {

		Template template = templateConf.getTemplate(model.TEMPLATE_NAME);

		String generatedFilename = model.getSorterName();

		synchronized (this){
			if( generatedSorter.getOrDefault(generatedFilename, false) ){
				if (LOG.isDebugEnabled()) {
					LOG.debug("Served from cache : "+generatedFilename);
				}
				return generatedFilename;
			}

			FileOutputStream fs = new FileOutputStream(this.getPathToGeneratedCode(generatedFilename));

			Writer output = new OutputStreamWriter(fs);
			Map templateVariables = model.getTemplateVariables();
			template.process(templateVariables, output);

			fs.close();
			output.close();

			generatedSorter.put(generatedFilename, true);
		}

		return generatedFilename;
	}

	/**
	 * Prepare directory for storing generated code
	 * @return path of the directory */
	private String prepareDirectoryGeneratedCode() throws IOException {
		String temporaryDir = EnvironmentInformation.getTemporaryFileDirectory();
		File dirForGeneratedCode = new File(temporaryDir + "/flink-codegeneration");

		if (!dirForGeneratedCode.exists()) {
			LOG.debug("Creating diretory for generated code at : "+ dirForGeneratedCode.getAbsolutePath());
			boolean res = dirForGeneratedCode.mkdir();
			if(!res){
				throw new IOException("Can't create temporary directory for generated code : " + dirForGeneratedCode.getAbsolutePath());
			}
		} else {
			LOG.debug( dirForGeneratedCode.getAbsolutePath() +" already exists, so just clean it up");
			FileUtils.cleanDirectory(dirForGeneratedCode);
		}

		return dirForGeneratedCode.getAbsolutePath();
	}

	/**
	 * Utility method to get the File object of the given filename
	 * return an object of File of the given file
	 */
	public File getPathToGeneratedCode(String filename){
		return new File(this.dirForGeneratedCode + "/" + filename + ".java");
	}
}
