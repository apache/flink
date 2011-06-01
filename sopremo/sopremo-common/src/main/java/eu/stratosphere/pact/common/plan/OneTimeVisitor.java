package eu.stratosphere.pact.common.plan;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Visitor adapter that guarantees to visit all {@link Visitable}s only once.
 * 
 * @author Arvid Heise
 * @param <T>
 *        the type of the Visitable
 */
public class OneTimeVisitor<T extends Visitable<T>> implements Visitor<T> {
	private final Map<T, Object> visitedNodes = new IdentityHashMap<T, Object>();

	private final Visitor<T> visitor;

	/**
	 * Initializes the OneTimeVisitor adapter with the given visitor. The visitor callback is invoked when this
	 * OneTimeVisitor is used to traverse a graph of {@link Visitable}s.
	 * 
	 * @param visitor
	 *        the wrapped visitor
	 */
	public OneTimeVisitor(Visitor<T> visitor) {
		this.visitor = visitor;
	}

	@Override
	public void postVisit(T visitable) {
		if (this.visitedNodes.get(visitable) == Boolean.TRUE)
			return;
		this.visitedNodes.put(visitable, Boolean.TRUE);
		this.visitor.postVisit(visitable);
	}

	@Override
	public boolean preVisit(T visitable) {
		if (this.visitedNodes.containsKey(visitable))
			return false;
		this.visitedNodes.put(visitable, Boolean.FALSE);
		this.visitor.preVisit(visitable);
		return true;
	}
}