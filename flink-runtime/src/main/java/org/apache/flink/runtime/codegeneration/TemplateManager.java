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

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TemplateManager {
	public static String RESOURCE_PATH  = "/Users/heytitle/projects/apache-flink/flink-runtime/resources";
	public static String TEMPLATE_PATH  = RESOURCE_PATH + "/templates";

	// TODO: generated this folder if it doesn't exist
	public static String GENERATING_PATH = RESOURCE_PATH + "/generatedcode";

	private static TemplateManager templateManager;

	private Configuration templateConf;
	private Map<String,Boolean> generatedSorter;

	public TemplateManager() throws IOException {
		templateConf = new Configuration();
		templateConf.setDirectoryForTemplateLoading(new File(TEMPLATE_PATH));
		templateConf.setDefaultEncoding("UTF-8");
		templateConf.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

		generatedSorter = new HashMap<>();
	}


	public static TemplateManager getInstance() throws IOException {
		if( templateManager == null ) {
			return new TemplateManager();
		}

		return templateManager;
	}


	public String getGeneratedCode(SorterTemplateModel model) throws IOException, TemplateException {
		Template template = templateConf.getTemplate(model.TEMPLATE_NAME);

		String generatedFilename = model.getSorterName();

		if( generatedSorter.getOrDefault(generatedFilename, false) ){
			System.out.println("Serve from cache!");
			return generatedFilename;
		}

		FileOutputStream fs = new FileOutputStream(new File(GENERATING_PATH +"/"+generatedFilename+".java"));

		Writer output = new OutputStreamWriter(fs);
		Map templateVariables = model.getTemplateVariables();
		template.process(templateVariables, output);

		fs.close();
		output.close();

		generatedSorter.put(generatedFilename, true);

		return generatedFilename;
	}
}
