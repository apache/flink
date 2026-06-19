public class TestIfMultipleSingleLineStatementRewrite {
    public void myFun1(int[] a, int[] b) throws RuntimeException {

        if (a[0] == 0) {
            myFun1_0_0_rewriteGroup2(a, b);

            myFun1_0_0(a, b);
        } else {
            myFun1_0_4(a, b);
        }
    }

    void myFun1_0_0_1_2(int[] a, int[] b) throws RuntimeException {
        a[21] = 1;
        a[22] = 1;
    }

    void myFun1_0_4(int[] a, int[] b) throws RuntimeException {
        a[0] = b[0];
        a[1] = b[1];
        a[2] = b[2];
    }

    void myFun1_0_0_1_3(int[] a, int[] b) throws RuntimeException {
        a[23] = b[2];
        a[24] = b[2];
    }

    void myFun1_0_0_1(int[] a, int[] b) throws RuntimeException {
        a[11] = b[0];
        a[12] = b[0];
    }

    void myFun1_0_0(int[] a, int[] b) throws RuntimeException {
        a[13] = b[0];
        a[14] = b[0];
    }

    void myFun1_0_0_rewriteGroup2(int[] a, int[] b) throws RuntimeException {
        myFun1_0_0_1(a, b);
        if (a[2] == 0) {
            myFun1_0_0_1_2(a, b);
        } else {
            myFun1_0_0_1_3(a, b);
        }
    }

}
