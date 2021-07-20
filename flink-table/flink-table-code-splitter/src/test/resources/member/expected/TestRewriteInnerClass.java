public class TestRewriteInnerClass {
int[] rewrite$0 = new int[3];

{
rewrite$0[1] = 1;
}

    
    
    

    public TestRewriteInnerClass(int arg) {
        rewrite$0[2] = arg;
    }

    public int myFun(int d) {
        int aa = rewrite$0[0];
        return rewrite$0[0] + aa + this.rewrite$0[1] + rewrite$0[2] + d;
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
            return innerA + aa + this.innerB + innerC + d + rewrite$0[0] + this.rewrite$0[1] + rewrite$0[2];
        }
    }
}
