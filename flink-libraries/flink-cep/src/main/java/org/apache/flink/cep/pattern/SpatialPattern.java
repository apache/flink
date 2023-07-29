package org.apache.flink.cep.pattern;

import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.spatial.GeometryEvent;

/**
 * Base class for a spatial pattern definition.
 *
 * <p>A pattern definition is used by {@link org.apache.flink.cep.nfa.compiler.NFACompiler} to
 * create a {@link NFA}.
 *
 * <pre>{@code
 * SpatialPattern<T, F> pattern = SpatialPattern.<T>begin("start")
 *   .next("middle").subtype(F.class)
 *   .followedBy("end").where(new MySpatialCondition());
 * }</pre>
 *
 * @param <T> Base type of the elements appearing in the pattern
 * @param <F> Subtype of T to which the current pattern operator is constrained
 */
public class SpatialPattern<T extends GeometryEvent, F extends T> extends Pattern<T, F> {

    protected SpatialPattern(
            String name,
            Pattern<T, ? extends T> previous,
            Quantifier.ConsumingStrategy consumingStrategy,
            AfterMatchSkipStrategy afterMatchSkipStrategy) {
        super(name, previous, consumingStrategy, afterMatchSkipStrategy);
    }
}
