public class TestIfMultipleSingleLineStatementRewrite {
    public void myFun1(int[] a, int[] b) throws RuntimeException {
        if (a[0] == 0) {
            a[11] = b[0];
            a[12] = b[0];
            if (a[2] == 0) {
                a[21] = 1;
                a[22] = 1;
            } else {
                a[23] = b[2];
                a[24] = b[2];
            }

            a[13] = b[0];
            a[14] = b[0];
        } else {
            a[0] = b[0];
            a[1] = b[1];
            a[2] = b[2];
        }
    }
}
