public class TestRewriteInnerClass {
    public int a;
    protected int b = 1;
    final int c;

    public TestRewriteInnerClass(int arg) {
        c = arg;
    }

    public int myFun(int d) {
        int aa = a;
        return a + aa + this.b + c + d;
    }

    public class InnerClass {
        public int innerA;
        protected int innerB = 1;
        final int innerC;

        public TestRewriteInnerClass(int arg) {
            this.innerC = arg;
        }

        public int myFun(int d) {
            int aa = innerA;
            return innerA + aa + this.innerB + innerC + d + a + this.b + c;
        }
    }
}
