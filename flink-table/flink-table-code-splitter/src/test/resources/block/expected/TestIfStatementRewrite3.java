public class TestIfStatementRewrite3 {
    public void myFun1(int[] a, int[] b) throws RuntimeException {

        if (a[0] == 0) {
            myFun1_0_0(a, b);
        } else if (a[1] == 22) {
            myFun1_0_1_2(a, b);
        } else if (a[3] == 0) {
            myFun1_0_1_3_4(a, b);
        } else if (a[4] == 0) {
            myFun1_0_1_3_5_6(a, b);
        } else {
            myFun1_0_1_3_5_7(a, b);
        }
    }

    void myFun1_0_1_3_4(int[] a, int[] b) throws RuntimeException {
        a[3] = b[3];
        a[33] = b[33];
    }

    void myFun1_0_1_2(int[] a, int[] b) throws RuntimeException {
        a[1] = b[12];
        a[2] = b[22];
    }

    void myFun1_0_1_3_5_6(int[] a, int[] b) throws RuntimeException {
        a[4] = b[4];
        a[44] = b[44];
    }

    void myFun1_0_1_3_5_7(int[] a, int[] b) throws RuntimeException {
        a[0] = b[0];
        a[1] = b[1];
        a[2] = b[2];
    }

    void myFun1_0_0(int[] a, int[] b) throws RuntimeException {
        a[0] = 1;
        a[1] = 1;
    }

}
