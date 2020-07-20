package org.apache.flink.api.scala.typeutils;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

/**
 * A {@link TypeSerializerSnapshot} for the Scala {@link SealedTraitSerializer}.
 */
public class ScalaSealedTraitSerializerSnapshot<T> extends CompositeTypeSerializerSnapshot<T, SealedTraitSerializer<T>> {

	public static final int VERSION = 3;

	Class<?>[] subtypeClasses;
	SealedTraitSerializer<T> instance;

	/**
	 * Snapshot read constructor
	 */
	public ScalaSealedTraitSerializerSnapshot() {
		super(SealedTraitSerializer.class);
	}

	/**
	 * Snapshot write constructor
	 * @param instance the serializer instance to snapshot
	 */
	public ScalaSealedTraitSerializerSnapshot(SealedTraitSerializer<T> instance) {
		super(instance);
		this.instance = instance;
	}

	@Override
	public void writeOuterSnapshot(DataOutputView out) throws IOException {
		out.writeInt(instance.subtypeClasses().length);
		for (Class<?> clazz : instance.subtypeClasses()) {
			out.writeUTF(clazz.getName());
		}
	}

	@Override
	public void readOuterSnapshot(int readOuterSnapshotVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		int subtypeCount = in.readInt();
		subtypeClasses = new Class<?>[subtypeCount];
		for (int i = 0; i < subtypeCount; i++) {
			subtypeClasses[i] = InstantiationUtil.resolveClassByName(in, userCodeClassLoader);
		}
	}

	@Override
	public SealedTraitSerializer<T> createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
		return new SealedTraitSerializer<T>(subtypeClasses, (TypeSerializer<T>[]) nestedSerializers);
	}

	@Override
	public TypeSerializer<?>[] getNestedSerializers(SealedTraitSerializer<T> outerSerializer) {
		return outerSerializer.subtypeSerializers();
	}

	@Override
	public int getCurrentOuterSnapshotVersion() {
		return VERSION;
	}

	@Override
	public OuterSchemaCompatibility resolveOuterSchemaCompatibility(SealedTraitSerializer<T> newSerializer) {
		// arity change is not supported by the underlying CompositeTypeSerializerSnapshot
		if (subtypeClasses.length != newSerializer.subtypeClasses().length) {
			return OuterSchemaCompatibility.INCOMPATIBLE;
		} else {
			boolean compatible = true;
			for (int i = 0; (i < subtypeClasses.length) && compatible; i += 1) {
				if (subtypeClasses[i] != newSerializer.subtypeClasses()[i]) compatible = false;
			}
			if (compatible)
				return OuterSchemaCompatibility.COMPATIBLE_AS_IS;
			 else
				return OuterSchemaCompatibility.INCOMPATIBLE;
		}
	}
}
