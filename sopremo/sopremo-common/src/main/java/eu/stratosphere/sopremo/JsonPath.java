package eu.stratosphere.sopremo;

public class JsonPath {
	private JsonPath selector;

	public JsonPath(JsonPath selector) {
		this.selector = selector;
	}

	public JsonPath() {
		this(null);
	}

	public void setSelector(JsonPath selector) {
		this.selector = selector;
	}

	protected void toString(StringBuilder builder) {
		if (selector != null)
			selector.toString(builder);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		toString(builder);
		return builder.toString();
	}

	public static class IdentifierAccess extends JsonPath {
		private String identifier;

		public IdentifierAccess(String identifier) {
			this.identifier = identifier;
		}

		@Override
		protected void toString(StringBuilder builder) {
			builder.append(identifier);
			super.toString(builder);
		}
	}

	public static class Constant extends JsonPath {
		private Object constant;

		public Constant(Object constant) {
			this.constant = constant;
		}

		@Override
		protected void toString(StringBuilder builder) {
			builder.append(constant);
			super.toString(builder);
		}
	}

	public static class FieldAccess extends JsonPath {

		private String field;

		public FieldAccess(String field) {
			this.field = field;
		}

		@Override
		protected void toString(StringBuilder builder) {
			builder.append('.');
			builder.append(field);
			super.toString(builder);
		}
	}

	public static class ArrayAccess extends JsonPath {

		private int startIndex, endIndex;

		public ArrayAccess(int startIndex, int endIndex) {
			this.startIndex = startIndex;
			this.endIndex = endIndex;
		}

		public ArrayAccess(int index) {
			this(index, index);
		}

		@Override
		protected void toString(StringBuilder builder) {
			builder.append('[');
			builder.append(startIndex);
			if (startIndex != endIndex) {
				builder.append(':');
				builder.append(endIndex);
			}
			builder.append(']');
			super.toString(builder);
		}
	}

	public static class Function extends JsonPath {

		private String name;

		private JsonPath[] params;

		public Function(String name, JsonPath... params) {
			this.name = name;
			this.params = params;
		}

		@Override
		protected void toString(StringBuilder builder) {
			builder.append(name);
			builder.append('(');
			for (int index = 0; index < params.length; index++) {
				builder.append(params[index]);
				if (index < params.length - 1)
					builder.append(", ");
			}
			builder.append(')');
		}
	}

	public static class Arithmetic extends JsonPath {

		private ArithmeticOperator operator;

		private JsonPath op1, op2;

		public Arithmetic(JsonPath op1, ArithmeticOperator operator, JsonPath op2) {
			this.operator = operator;
			this.op1 = op1;
			this.op2 = op2;
		}

		@Override
		protected void toString(StringBuilder builder) {
			builder.append(op1);
			builder.append(' ');
			builder.append(operator);
			builder.append(' ');
			builder.append(op2);
		}
	}
}

enum ArithmeticOperator {
	PLUS("+"), MINUS("-"), MULTIPLY("*"), DIVIDE("/");
	
	private final String sign;

	ArithmeticOperator(String sign) {
		this.sign = sign;
	}

	@Override
	public String toString() {
		return sign;
	}
}
