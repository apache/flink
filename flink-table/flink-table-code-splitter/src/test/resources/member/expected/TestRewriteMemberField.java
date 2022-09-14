public class TestRewriteMemberField {
String[] rewrite$0 = new String[3];
int[] rewrite$1 = new int[3];
long[] rewrite$2 = new long[2];

{
rewrite$1[1] = 1;
rewrite$0[1] = "GGGGG";
}

    
    
    
    
    
    
    
    

    public TestRewriteMemberField(String s) {
        this.rewrite$0[0] = s;
    }

    public String myFun(String h) {
        int aa = rewrite$1[0];
        return rewrite$0[0] + aa + this.rewrite$0[2] + rewrite$1[0] + String.valueOf(rewrite$1[1]);
    }
}
