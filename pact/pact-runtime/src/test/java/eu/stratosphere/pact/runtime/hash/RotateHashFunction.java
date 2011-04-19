package eu.stratosphere.pact.runtime.hash;

public class RotateHashFunction implements HashFunction {

	@Override
	public int hash(int code, int level) {
		code = Integer.rotateLeft(code, level*11);
		
		code = (code + 0x7ed55d16) + (code << 12);
        code = (code ^ 0xc761c23c) ^ (code >>> 19);
        code = (code + 0x165667b1) + (code << 5);
        code = (code + 0xd3a2646c) ^ (code << 9);
        code = (code + 0xfd7046c5) + (code << 3);
        code = (code ^ 0xb55a4f09) ^ (code >>> 16);
        return code >= 0 ? code : - (code + 1);
	}

}
