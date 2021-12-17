public class TestNotRewriteLocalVariable {
    int a = 1;
    int b = 2;
    int c = 3;

    public TestNotRewriteFunctionParameter() {
        int b = this.b;
        int c = this.c;
        System.out.println(this.c + b + c + this.b);
    }

    public int myFun() {
        int a = this.a;
        return this.a + a + this.b + c;
    }
}
