package eu.stratosphere.sopremo;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.METHOD })
public @interface Name {
	String[] noun() default {};

	String[] verb() default {};

	String[] adjective() default {};

	String[] preposition() default {};
	
	public static enum NameType {
		NOUN {
			@Override
			public String[] get(Name name) {
				return name.noun();
			}
		}, VERB {
			@Override
			public String[] get(Name name) {
				return name.verb();
			}
		}, ADJECTIVE {
			@Override
			public String[] get(Name name) {
				return name.adjective();
			}
		}, PREPOSITION {
			@Override
			public String[] get(Name name) {
				return name.preposition();
			}
		};
		
		public abstract String[] get(Name name);
	}
}
