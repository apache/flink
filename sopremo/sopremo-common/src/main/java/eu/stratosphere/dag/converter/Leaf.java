package eu.stratosphere.dag.converter;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Gives the {@link GraphConverter} the hint that no child elements need to be processed, since the annotated
 * {@link NodeConverter} is considered to be leaf of the graph.
 * 
 * @author Arvid Heise
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface Leaf {

}
