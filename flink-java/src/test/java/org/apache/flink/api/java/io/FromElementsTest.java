package org.apache.flink.api.java.io;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.junit.Test;

public class FromElementsTest {

	@Test
	public void fromElementsWithBaseTypeTest1() {
		ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
		executionEnvironment.fromElements(ParentType.class, new SubType(1, "Java"), new ParentType(1, "hello"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void fromElementsWithBaseTypeTest2() {
		ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
		executionEnvironment.fromElements(SubType.class, new SubType(1, "Java"), new ParentType(1, "hello"));
	}

	public static class ParentType {
		int num;
		String string;
		public ParentType(int num, String string) {
			this.num = num;
			this.string = string;
		}
	}

	public static class SubType extends ParentType{
		public SubType(int num, String string) {
			super(num, string);
		}
	}
}
