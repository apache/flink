package eu.stratosphere.nephele.instance;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.io.IOReadableWritable;

/**
 * An instance type description provides details of instance type. Is can comprise both the hardware description from
 * the instance type description (as provided by the operator/administrator of the instance) as well as the actual
 * hardware description which has been determined on the compute instance itself.
 * 
 * @author warneke
 */
public final class InstanceTypeDescription implements IOReadableWritable {

	/**
	 * The instance type.
	 */
	private InstanceType instanceType = null;

	/**
	 * The hardware description as created by the {@link InstanceManager}.
	 */
	private HardwareDescription hardwareDescription = null;

	/**
	 * The maximum number of available instances of this type.
	 */
	private int maximumNumberOfAvailableInstances = 0;

	/**
	 * Public default constructor required for serialization process.
	 */
	public InstanceTypeDescription() {
	}

	/**
	 * Constructs a new instance type description.
	 * 
	 * @param instanceType
	 *        the instance type
	 * @param hardwareDescription
	 *        the hardware description as created by the {@link InstanceManager}
	 * @param maximumNumberOfAvailableInstances
	 *        the maximum number of available instances of this type
	 */
	InstanceTypeDescription(final InstanceType instanceType, final HardwareDescription hardwareDescription,
			final int maximumNumberOfAvailableInstances) {

		this.instanceType = instanceType;
		this.hardwareDescription = hardwareDescription;
		this.maximumNumberOfAvailableInstances = maximumNumberOfAvailableInstances;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		if (this.instanceType == null) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			this.instanceType.write(out);
		}

		if (this.hardwareDescription == null) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			this.hardwareDescription.write(out);
		}

		out.writeInt(this.maximumNumberOfAvailableInstances);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		if (in.readBoolean()) {
			this.instanceType = new InstanceType();
			this.instanceType.read(in);
		} else {
			this.instanceType = null;
		}

		if (in.readBoolean()) {
			this.hardwareDescription = new HardwareDescription();
			this.hardwareDescription.read(in);
		}

		this.maximumNumberOfAvailableInstances = in.readInt();
	}

	/**
	 * Returns the hardware description as created by the {@link InstanceManager}.
	 * 
	 * @return the instance's hardware description or <code>null</code> if no description is available
	 */
	public HardwareDescription getHardwareDescription() {
		return this.hardwareDescription;
	}

	/**
	 * Returns the instance type as determined by the {@link InstanceManager}.
	 * 
	 * @return the instance type
	 */
	public InstanceType getInstanceType() {
		return this.instanceType;
	}

	/**
	 * Returns the maximum number of instances the {@link InstanceManager} can at most allocate of this instance type
	 * (i.e. when no other jobs are occupying any resources).
	 * 
	 * @return the maximum number of instances of this type or <code>-1</code> if the number is unknown to the
	 *         {@link InstanceManager}
	 */
	public int getMaximumNumberOfAvailableInstances() {
		return this.maximumNumberOfAvailableInstances;
	}
}
