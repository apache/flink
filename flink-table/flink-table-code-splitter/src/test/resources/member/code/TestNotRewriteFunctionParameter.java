public class TestNotRewriteFunctionParameter {
    int a = 1;
    int b;
    int c;

    public TestNotRewriteFunctionParameter(int b, int c) {
        this.b = b;
        this.c = c;
        System.out.println(this.c + b + c + this.b);
    }

    public int myFun(int a) {
        this.a = a;
        return this.a + a + this.b + c;
    }
}
