public class TestNotRewriteIfStatementInFunctionWithReturnValue {
    public int myFun(int[] a, int[] b) throws RuntimeException {
        if (a[0] == 0) {
            a[0] = 1;
            if (a[1] == 0) {
                a[1] = 1;
                if (a[2] == 0) {
                    a[2] = 1;
                } else {
                    a[2] = b[2];
                }
            } else {
                a[1] = b[1];
                a[2] = b[2];
            }
        } else {
            a[0] = b[0];
            a[1] = b[1];
            a[2] = b[2];
        }
        return a[0] + a[1] + a[2];
    }
}
