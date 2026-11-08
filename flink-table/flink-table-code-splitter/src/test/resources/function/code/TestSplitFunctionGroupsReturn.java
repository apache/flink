public class TestSplitFunctionGroupsReturn {
    public void myFun(int[] a, int[] b) {
        a[0] += b[0];
        a[1] += b[1];
        a[2] += b[2];
        a[3] += b[3];
        a[4] += b[4];
        a[5] += b[5];
        a[6] += b[6];
        a[7] += b[7];
        if (a[8] > 0) { a[0] = 100; return; }
        a[8] += b[8];
        a[9] += b[9];
        a[10] += b[10];
        a[11] += b[11];
        a[12] += b[12];
        a[13] += b[13];
        a[14] += b[14];
        a[15] += b[15];
    }
}
