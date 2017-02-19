package org.apache.flink.codegeneration;


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

/**
 * Created by heytitle on 2/19/17.
 */
public class TemplateManager {
	public static String RESOURCE_PATH  = "/Users/heytitle/projects/apache-flink/flink-runtime/resources";
	public static String TEMPLATE_PATH  = RESOURCE_PATH + "/templates";

	/* TODO: generated this folder if it doesn't exist */
	public static String GENERATING_PATH = RESOURCE_PATH + "/generatedcode";

	public static String getGeneratedCode(SorterTemplateModel model) throws IOException, TemplateException {
		String dummyName = "SomethingSorter";
		/* This configuration  should be created once */
		Configuration cfg = new Configuration();
		cfg.setDirectoryForTemplateLoading(new File(TEMPLATE_PATH));
		cfg.setDefaultEncoding("UTF-8");
		cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

		Map<String, Object> root = new HashMap<>();
		root.put("name", dummyName);

		Template template = cfg.getTemplate("sorter.ftlh");

		/* Overwrite existing file */
		FileOutputStream fs = new FileOutputStream(new File(GENERATING_PATH +"/"+dummyName+".java"), false);

		Writer out = new OutputStreamWriter(fs);
		template.process(root, out);

		out.flush();

		return dummyName;
	}
}
