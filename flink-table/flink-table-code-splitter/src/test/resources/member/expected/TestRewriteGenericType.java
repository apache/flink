public class TestRewriteGenericType {
java.util.List<Integer>[] rewrite$0 = new java.util.List[1];
java.util.List<String>[] rewrite$1 = new java.util.List[2];

{
rewrite$1[0] = new java.util.ArrayList<>();
rewrite$0[0] = new java.util.ArrayList<>();
rewrite$1[1] = new java.util.ArrayList<>();
}

    
    
    

    public String myFun() {
        String aa = rewrite$1[0].get(0);
        long bb = rewrite$0[0].get(1);
        String cc = rewrite$1[1].get(2);
        return cc + bb + aa;
    }
}
