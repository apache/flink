public class TestNotRewriteFunctionParameter {
int[] rewrite$0 = new int[3];

{
rewrite$0[0] = 1;
}

    
    
    

    public TestNotRewriteFunctionParameter(int b, int c) {
        this.rewrite$0[1] = b;
        this.rewrite$0[2] = c;
        System.out.println(this.rewrite$0[2] + b + c + this.rewrite$0[1]);
    }

    public int myFun(int a) {
        this.rewrite$0[0] = a;
        return this.rewrite$0[0] + a + this.rewrite$0[1] + rewrite$0[2];
    }
}
