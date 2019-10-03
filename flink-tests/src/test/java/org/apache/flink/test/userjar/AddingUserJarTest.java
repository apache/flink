package org.apache.flink.test.userjar;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;

public class AddingUserJarTest extends AbstractTestBase {
	private String testJar = "target/userjar-test-jar.jar";

	@Test
	public void testBatchUserJar() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.registerUserJarFile(testJar);
		env.fromElements("1").map(new UdfMapper()).count();
	}

	@Test
	public void testStreamingUserJar() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.registerUserJarFile(testJar);
		env.fromElements("1").map(new UdfMapper()).addSink(new DiscardingSink<>());
		env.execute();
	}

	private static class UdfMapper extends RichMapFunction<String, String>{
		private Object mapper;
		private Method mapFunc;

		@Override
		public void open(Configuration parameters) throws Exception {
			ClassLoader loader = getRuntimeContext().getUserCodeClassLoader();
			Class clazz = Class.forName("org.apache.flink.test.userjar.MapFunc", false, loader);
			mapper = clazz.newInstance();
			mapFunc = clazz.getDeclaredMethod("eval", String.class);
		}

		@Override
		public String map(String value) throws Exception {
			String hello = (String) mapFunc.invoke(mapper, value);
			assertEquals("Hello Flink!", hello);
			return hello;
		}
	}
}
