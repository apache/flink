public class TestNotRewriteMember {
    int a;
    private static int b;
    public final int c = 1;
    // not rewrite static member
    public static final int d = 2;
    private int e;
    // special varaible name used by code generators, does not rewrite this
    private Object[] references;

    public TestNotRewriteMember(Object[] references) {
        this.references = references;
    }

    public int myFun() {
        return a + b + this.c + TestNotRewriteMember.d + e;
    }
}
