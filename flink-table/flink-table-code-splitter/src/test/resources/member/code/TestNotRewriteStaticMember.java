public class TestNotRewriteStaticMember {
    int a;
    private static int b;
    public final int c = 1;
    public static final int d = 2;
    private int e;

    public int myFun() {
        return a + b + this.c + TestNotRewriteStaticMember.d + e;
    }
}
