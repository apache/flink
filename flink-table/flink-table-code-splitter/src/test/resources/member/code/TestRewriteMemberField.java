public class TestRewriteMemberField {
    public int a;
    private long b;
    protected int c = 1;
    long d;
    final int e;
    public final String f;
    private final String g = "GGGGG";
    String h;

    public TestRewriteMemberField(String s) {
        this.f = s;
    }

    public String myFun(String h) {
        int aa = a;
        return f + aa + this.h + a + String.valueOf(c);
    }
}
