package eu.stratosphere.sopremo.expressions;

public enum Scope {
	ANY,

	OBJECT(ANY), ARRAY(ANY), PRIMITIVE(ANY),

	STRING(PRIMITIVE), NUMBER(PRIMITIVE);

	private final Scope parent;

	private Scope() {
		this(null);
	}

	private Scope(Scope parent) {
		this.parent = parent;
	}
}