package eu.stratosphere.sopremo.operator;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to specify names for {@link ElementType#TYPE}s and {@link ElementType#METHOD}s
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.METHOD })
public @interface Name {
	String[] noun() default {};

	String[] verb() default {};

	String[] adjective() default {};

	String[] preposition() default {};

	/**
	 * Enumeration that specifies the type of the name.</br> <li>{@link NameType#NOUN} <li>{@link NameType#VERB} <li>
	 * {@link NameType#ADJECTIVE} <li>{@link NameType#PREPOSITION}
	 */
	public static enum NameType {
		/**
		 * Specifies that the name is a noun.
		 */
		NOUN {
			@Override
			public String[] get(final Name name) {
				return name.noun();
			}
		},
		/**
		 * Specifies that the name is a verb.
		 */
		VERB {
			@Override
			public String[] get(final Name name) {
				return name.verb();
			}
		},
		/**
		 * Specifies that the name is an adjective.
		 */
		ADJECTIVE {
			@Override
			public String[] get(final Name name) {
				return name.adjective();
			}
		},
		/**
		 * Specifies that the name is a preposition.
		 */
		PREPOSITION {
			@Override
			public String[] get(final Name name) {
				return name.preposition();
			}
		};

		/**
		 * Gets the name.
		 * 
		 * @param name
		 *        the name that is specified by this annotation.
		 * @return the name
		 */
		public abstract String[] get(Name name);
	}
}
