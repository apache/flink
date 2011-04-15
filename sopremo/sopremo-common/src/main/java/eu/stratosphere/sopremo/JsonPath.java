package eu.stratosphere.sopremo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.stratosphere.dag.Navigator;
import eu.stratosphere.reflect.TypeHandler;
import eu.stratosphere.reflect.TypeSpecificHandler;

public class JsonPath implements Cloneable {
	public static final JsonPath Unknown = new JsonPath.IdentifierAccess("?");
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

	public JsonPath getSelector() {
		return selector;
	}

	protected void toString(StringBuilder builder) {
		if (this.selector != null)
			this.selector.toString(builder);
	}

	private static TypeSpecificHandler<JsonPath, JsonPath, TypeHandler<JsonPath, JsonPath>> PathReplacer = new TypeSpecificHandler<JsonPath, JsonPath, TypeHandler<JsonPath, JsonPath>>();

	static {
		PathReplacer.register(new TypeHandler<JsonPath, JsonPath>() {
			public JsonPath replace(JsonPath path, List<Mapping> mapping, JsonPath toReplace,
					JsonPath replaceFragment) {
				if (path.isPrefix(toReplace)) {
					JsonPath newPath = replaceFragment.clone();
					newPath.setSelector(path.getSelector(toReplace.getDepth()));
					return newPath;
				}
				return path;
			}
		}, JsonPath.class);
	}

	public static JsonPath replace(JsonPath start, JsonPath toReplace, JsonPath replaceFragment) {
		return PathReplacer.handleRecursively(new JsonPathNavigator(), start, toReplace, replaceFragment);
	}

	public boolean isPrefix(JsonPath prefix) {
		if (!equals(prefix))
			return false;
		if (getSelector() == null)
			return prefix.getSelector() == null;
		if (prefix.getSelector() == null)
			return true;
		return getSelector().isPrefix(prefix.getSelector());
	}

	public int getDepth() {
		if (getSelector() == null)
			return 1;
		return 1 + getSelector().getDepth();
	}

	public JsonPath getSelector(int distance) {
		if (distance < 0)
			distance += getDepth();
		if (distance == 0)
			return this;
		if (getSelector() == null)
			return null;
		return getSelector().getSelector(distance - 1);
	}

	@Override
	public JsonPath clone() {
		try {
			JsonPath clone = (JsonPath) super.clone();
			if (getSelector() != null)
				clone.setSelector(clone.getSelector().clone());
			return clone;
		} catch (CloneNotSupportedException e) {
			throw new IllegalStateException("should never happen", e);
		}
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		this.toString(builder);
		return builder.toString();
	}

	public static class IdentifierAccess extends JsonPath {
		private String identifier;

		public IdentifierAccess(String identifier) {
			this.identifier = identifier;
		}

		@Override
		protected void toString(StringBuilder builder) {
			builder.append(this.identifier);
			super.toString(builder);
		}

		@Override
		public int hashCode() {
			return 31 + identifier.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (getClass() != obj.getClass())
				return false;
			return identifier.equals(((IdentifierAccess) obj).identifier);
		}

	}

	public static class Input extends JsonPath {
		private int index;

		public Input(int index) {
			this.index = index;
		}

		public int getIndex() {
			return index;
		}

		@Override
		protected void toString(StringBuilder builder) {
			builder.append("in").append(this.index + 1);
			super.toString(builder);
		}

		@Override
		public int hashCode() {
			return 37 + index;
		}

		@Override
		public boolean equals(Object obj) {
			if (getClass() != obj.getClass())
				return false;
			return index == ((Input) obj).index;
		}

	}

	public static class Constant extends JsonPath {
		// TODO: adjust to json model
		private Object constant;

		public Constant(Object constant) {
			this.constant = constant;
		}

		public String asString() {
			return this.constant.toString();
		}

		public int asInt() {
			if (this.constant instanceof Number)
				return ((Number) this.constant).intValue();
			return Integer.parseInt(this.constant.toString());
		}

		@Override
		protected void toString(StringBuilder builder) {
			if (this.constant instanceof CharSequence)
				builder.append("\'").append(this.constant).append("\'");
			else
				builder.append(this.constant);
			super.toString(builder);
		}

		@Override
		public int hashCode() {
			return 41 + constant.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (getClass() != obj.getClass())
				return false;
			return constant.equals(((Constant) obj).constant);
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
			builder.append(this.field);
			super.toString(builder);
		}

		@Override
		public int hashCode() {
			return 43 + field.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (getClass() != obj.getClass())
				return false;
			return field.equals(((FieldAccess) obj).field);
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

		public ArrayAccess() {
			this(0, -1);
		}

		public boolean isSelectingAll() {
			return startIndex == 0 && endIndex == -1;
		}

		@Override
		protected void toString(StringBuilder builder) {
			builder.append('[');
			if (isSelectingAll())
				builder.append('*');
			else {
				builder.append(this.startIndex);
				if (this.startIndex != this.endIndex) {
					builder.append(':');
					builder.append(this.endIndex);
				}
			}
			builder.append(']');
			super.toString(builder);
		}

		@Override
		public int hashCode() {
			return (47 + startIndex) * 47 + endIndex;
		}

		@Override
		public boolean equals(Object obj) {
			if (getClass() != obj.getClass())
				return false;
			return startIndex == ((ArrayAccess) obj).startIndex && endIndex == ((ArrayAccess) obj).endIndex;
		}
	}

	public static class ArrayCreation extends JsonPath {
		private JsonPath[] elements;

		public ArrayCreation(JsonPath... elements) {
			this.elements = elements;
		}

		public ArrayCreation(List<JsonPath> elements) {
			this.elements = elements.toArray(new JsonPath[elements.size()]);
		}

		@Override
		protected void toString(StringBuilder builder) {
			builder.append(Arrays.toString(elements));
			super.toString(builder);
		}

		@Override
		public int hashCode() {
			return 53 + Arrays.hashCode(elements);
		}

		@Override
		public boolean equals(Object obj) {
			if (getClass() != obj.getClass())
				return false;
			return Arrays.equals(elements, ((ArrayCreation) obj).elements);
		}
	}

	public static class ObjectCreation extends JsonPath {
		private ValueAssignment[] assignments;

		public ObjectCreation(ValueAssignment... assignments) {
			this.assignments = assignments;
		}

		public ObjectCreation(List<ValueAssignment> assignments) {
			this.assignments = assignments.toArray(new ValueAssignment[assignments.size()]);
		}

		@Override
		protected void toString(StringBuilder builder) {
			builder.append(Arrays.toString(assignments));
			super.toString(builder);
		}

		@Override
		public int hashCode() {
			return 53 + Arrays.hashCode(assignments);
		}

		@Override
		public boolean equals(Object obj) {
			if (getClass() != obj.getClass())
				return false;
			return Arrays.equals(assignments, ((ObjectCreation) obj).assignments);
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
			builder.append(this.name);
			builder.append('(');
			for (int index = 0; index < this.params.length; index++) {
				builder.append(this.params[index]);
				if (index < this.params.length - 1)
					builder.append(", ");
			}
			builder.append(')');
		}

		@Override
		public int hashCode() {
			return (53 + name.hashCode()) * 53 + Arrays.hashCode(params);
		}

		@Override
		public boolean equals(Object obj) {
			if (getClass() != obj.getClass())
				return false;
			return name.equals(((Function) obj).name) && Arrays.equals(params, ((Function) obj).params);
		}
	}

	public static class Arithmetic extends JsonPath {
		public static enum ArithmeticOperator {
			PLUS("+"), MINUS("-"), MULTIPLY("*"), DIVIDE("/");

			private final String sign;

			ArithmeticOperator(String sign) {
				this.sign = sign;
			}

			@Override
			public String toString() {
				return this.sign;
			}
		}

		private ArithmeticOperator operator;

		private JsonPath op1, op2;

		public Arithmetic(JsonPath op1, ArithmeticOperator operator, JsonPath op2) {
			this.operator = operator;
			this.op1 = op1;
			this.op2 = op2;
		}

		@Override
		protected void toString(StringBuilder builder) {
			builder.append(this.op1);
			builder.append(' ');
			builder.append(this.operator);
			builder.append(' ');
			builder.append(this.op2);
		}

		@Override
		public int hashCode() {
			return ((59 + op1.hashCode()) * 59 + operator.hashCode()) * 59 + op2.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (getClass() != obj.getClass())
				return false;
			return op1.equals(((Arithmetic) obj).op1) && operator.equals(((Arithmetic) obj).operator)
				&& op2.equals(((Arithmetic) obj).op2);
		}
	}

	private static final class JsonPathNavigator implements Navigator<JsonPath> {
		private static final Iterable<JsonPath> EMPTY = new ArrayList<JsonPath>();

		@Override
		public Iterable<JsonPath> getConnectedNodes(JsonPath node) {
			if (node.getSelector() == null)
				return EMPTY;
			return Arrays.asList(node.getSelector());
		}
	}
}
