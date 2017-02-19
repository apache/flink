package org.apache.flink.codegeneration;

import freemarker.template.TemplateException;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.codehaus.janino.JavaSourceClassLoader;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * Created by heytitle on 2/19/17.
 */
public class SorterFactory {
	/* Set up janino */
	private static final ClassLoader cl = new JavaSourceClassLoader(
		SorterFactory.class.getClassLoader(),
		new File[] { new File(TemplateManager.GENERATING_PATH) },
		"UTF-8"
	);

	public static InMemorySorter createSorter(TypeSerializer serializer, TypeComparator comparator, List<MemorySegment> memory ) throws IOException, TemplateException, ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
		String className = TemplateManager.getGeneratedCode(new SorterTemplateModel());

		Object mySorter = cl.loadClass(className).getConstructor(
			TypeSerializer.class, TypeComparator.class, List.class
		).newInstance( serializer, comparator, memory);

		System.out.println(">> " + mySorter.toString());

		return (InMemorySorter)mySorter;
	}
}
