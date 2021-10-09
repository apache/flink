public class TestNotRewriteMember {
int[] rewrite$0 = new int[3];

{
rewrite$0[1] = 1;
}

    
    private static int b;
    
    // not rewrite static member
    public static final int d = 2;
    
    // special varaible name used by code generators, does not rewrite this
    private Object[] references;

    public TestNotRewriteMember(Object[] references) {
        this.references = references;
    }

    public int myFun() {
        return rewrite$0[0] + b + this.rewrite$0[1] + TestNotRewriteMember.d + rewrite$0[2];
    }
}
