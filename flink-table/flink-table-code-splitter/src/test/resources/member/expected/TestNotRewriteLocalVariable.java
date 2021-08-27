public class TestNotRewriteLocalVariable {
int[] rewrite$0 = new int[3];

{
rewrite$0[0] = 1;
rewrite$0[1] = 2;
rewrite$0[2] = 3;
}

    
    
    

    public TestNotRewriteFunctionParameter() {
        int b = this.rewrite$0[1];
        int c = this.rewrite$0[2];
        System.out.println(this.rewrite$0[2] + b + c + this.rewrite$0[1]);
    }

    public int myFun() {
        int a = this.rewrite$0[0];
        return this.rewrite$0[0] + a + this.rewrite$0[1] + rewrite$0[2];
    }
}
