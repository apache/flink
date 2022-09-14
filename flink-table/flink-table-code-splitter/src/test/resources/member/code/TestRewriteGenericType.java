public class TestRewriteGenericType {
    java.util.List<String> a = new java.util.ArrayList<>();
    java.util.List<Integer> b = new java.util.ArrayList<>();
    java.util.List<String> c = new java.util.ArrayList<>();

    public String myFun() {
        String aa = a.get(0);
        long bb = b.get(1);
        String cc = c.get(2);
        return cc + bb + aa;
    }
}
