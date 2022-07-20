public class TestRewriteReturnValue {
    public String fun1(int a, String b) {
        if (a > 0) {
            a += 5;
            return b + "test" + a;
        }
        a -= 5;
        return b + "test" + a;
    }

    public String fun2(int a, String b) throws Exception {
        if (a > 0) {
            a += 5;
            if (a > 100) {
                throw new RuntimeException();
            }
            return b + "test" + a;
        }
        a -= 5;
        return b + "test" + a;
    }
}
