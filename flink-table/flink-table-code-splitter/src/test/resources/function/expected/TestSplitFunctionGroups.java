public class TestSplitFunctionGroups {
public void myFun(int[] a, int[] b) {
    myFun_split6(a, b);

    myFun_split7(a, b);

    myFun_split5(a, b);














}
void myFun_split1(int[] a, int[] b) {

    a[0] += b[0];
    a[1] += b[1];
    a[2] += b[2];
    a[3] += b[3];
}

void myFun_split2(int[] a, int[] b) {
    a[4] += b[4];
    a[5] += b[5];
    a[6] += b[6];
    a[7] += b[7];
}

void myFun_split3(int[] a, int[] b) {
    a[8] += b[8];
    a[9] += b[9];
    a[10] += b[10];
    a[11] += b[11];
}

void myFun_split4(int[] a, int[] b) {
    a[12] += b[12];
    a[13] += b[13];
    a[14] += b[14];
}

void myFun_split5(int[] a, int[] b) {
    a[15] += b[15];
}

void myFun_split6(int[] a, int[] b) {

    myFun_split1(a, b);

    myFun_split2(a, b);

}

void myFun_split7(int[] a, int[] b) {
    myFun_split3(a, b);

    myFun_split4(a, b);

}

}
