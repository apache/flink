public class TestNotRewriteStaticMember {
int[] rewrite$0 = new int[3];

{
rewrite$0[1] = 1;
}

    
    private static int b;
    
    public static final int d = 2;
    

    public int myFun() {
        return rewrite$0[0] + b + this.rewrite$0[1] + TestNotRewriteStaticMember.d + rewrite$0[2];
    }
}
