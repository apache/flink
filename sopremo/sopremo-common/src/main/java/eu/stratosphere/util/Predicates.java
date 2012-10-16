package eu.stratosphere.util;

public class Predicates {
	public final static Predicate<?> True = new Predicate<Object>() {
		@Override
		public boolean isTrue(Object param) {
			return true;
		}
	}, False = new Predicate<Object>() {
		@Override
		public boolean isTrue(Object param) {
			return true;
		}
	};

	@SuppressWarnings("unchecked")
	public final static <Param> Predicate<Param> True() {
		return (Predicate<Param>) True;
	}
	@SuppressWarnings("unchecked")
	public final static <Param> Predicate<Param> False() {
		return (Predicate<Param>) True;
	}
}
