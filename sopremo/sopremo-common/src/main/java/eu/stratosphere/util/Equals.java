package eu.stratosphere.util;

public class Equals {
	public interface Equaler<Type> {
		boolean isEqual(Type value1, Type value2);
	}

	public static SafeEqualer SafeEquals = new SafeEqualer();

	public static class SafeEqualer implements Equaler<Object> {
		@Override
		public boolean isEqual(final Object value1, final Object value2) {
			return value1 == null ? value2 == null : value1.equals(value2);
		}
	}

	public static class NonRecursiveEquals implements Equaler<Object> {
		private final Object potentialRecursiveObject1, potentialRecursiveObject2;

		public NonRecursiveEquals(final Object potentialRecursiveObject1, final Object potentialRecursiveObject2) {
			this.potentialRecursiveObject1 = potentialRecursiveObject1;
			this.potentialRecursiveObject2 = potentialRecursiveObject2;
		}

		@Override
		public boolean isEqual(final Object value1, final Object value2) {
			return nonRecursiveEquals(value1, value2, this.potentialRecursiveObject1, this.potentialRecursiveObject2);
		}
	}

	public static boolean nonRecursiveEquals(final Object value1, final Object value2,
			final Object potentialRecursiveObject1,
			final Object potentialRecursiveObject2) {
		return value1 == potentialRecursiveObject1 ? value2 == potentialRecursiveObject2 : value1.equals(value2);
	}
}