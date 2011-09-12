package eu.stratosphere.sopremo;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.METHOD })
public @interface Property {
	boolean preferred() default false;

	boolean expert() default false;

	boolean hidden() default false;

	boolean input() default false;

	String[] description() default {};
}
