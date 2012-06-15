package eu.stratosphere.sopremo.base.join;

import eu.stratosphere.pact.common.stubs.Stub;

public class OuterJoin extends TwoSourceJoinBase<OuterJoin> {
	private static final long serialVersionUID = 317168181417121979L;

	private Mode mode = Mode.BOTH;

	public enum Mode {
		NONE, LEFT, RIGHT, BOTH;
	}

	public Mode getMode() {
		return mode;
	}

	/**
	 * Sets the mode of the outer join.
	 * 
	 * @param mode
	 *            the mode to set
	 */
	public void setMode(Mode mode) {
		if (mode == null) throw new NullPointerException("mode must not be null");

		this.mode = mode;
	}

	/**
	 * Sets the mode of the outer join.
	 * 
	 * @param mode
	 *            the mode to set
	 * @return this
	 */
	public OuterJoin withMode(Mode mode) {
		setMode(mode);
		return this;
	}

	/**
	 * Sets the mode of the outer join.
	 * 
	 * @param retainLeft
	 *            whether left side should be retained
	 * @param retainRight
	 *            whether right side should be retained
	 * @return this
	 */
	public OuterJoin withMode(boolean retainLeft, boolean retainRight) {
		int modeIndex = (retainLeft ? 1 : 0) + 2 * (retainRight ? 1 : 0);
		setMode(Mode.values()[modeIndex]);
		return this;
	}

	@Override
	protected Class<? extends Stub> getStubClass() {
		switch (mode) {
		case BOTH:
			return FullOuterJoin.Implementation.class;
		case RIGHT:
			return RightOuterJoin.Implementation.class;
		case LEFT:
			return LeftOuterJoin.Implementation.class;
		case NONE:
			return InnerJoin.Implementation.class;
		default:
			throw new IllegalStateException();
		}
	}
}