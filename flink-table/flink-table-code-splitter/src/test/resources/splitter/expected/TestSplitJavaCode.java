public class TestSplitJavaCode {
boolean[] rewrite$29 = new boolean[2];
int[][] rewrite$30 = new int[1][];
int[] rewrite$31 = new int[16];

{
rewrite$31[15] = 1;
        }























public TestSplitJavaCode(int[] b) {
    TestSplitJavaCode_split16(b);

    TestSplitJavaCode_split17(b);

    TestSplitJavaCode_split15(b);



















}
void TestSplitJavaCode_split9(int[] b) {

    this.rewrite$30[0] = b;
    rewrite$31[0] = this.rewrite$30[0][0] + b[0];
    System.out.println("b0 = " + rewrite$31[0]);
    rewrite$31[1] = this.rewrite$30[0][1] + b[1];
}

void TestSplitJavaCode_split10(int[] b) {
    System.out.println("b1 = " + rewrite$31[1]);
    rewrite$31[2] = this.rewrite$30[0][2] + b[2];
    System.out.println("b2 = " + rewrite$31[2]);
}

void TestSplitJavaCode_split11(int[] b) {
    rewrite$31[3] = this.rewrite$30[0][3] + b[3];
    System.out.println("b3 = " + rewrite$31[3]);
    rewrite$31[4] = this.rewrite$30[0][4] + b[4];
}

void TestSplitJavaCode_split12(int[] b) {
    System.out.println("b4 = " + rewrite$31[4]);
    rewrite$31[5] = this.rewrite$30[0][5] + b[5];
    System.out.println("b5 = " + rewrite$31[5]);
}

void TestSplitJavaCode_split13(int[] b) {
    rewrite$31[6] = this.rewrite$30[0][6] + b[6];
    System.out.println("b6 = " + rewrite$31[6]);
    rewrite$31[7] = this.rewrite$30[0][7] + b[7];
}

void TestSplitJavaCode_split14(int[] b) {
    System.out.println("b7 = " + rewrite$31[7]);
    rewrite$31[8] = this.rewrite$30[0][8] + b[8];
    System.out.println("b8 = " + rewrite$31[8]);
}

void TestSplitJavaCode_split15(int[] b) {
    rewrite$31[9] = this.rewrite$30[0][9] + b[9];
    System.out.println("b9 = " + rewrite$31[9]);
}

void TestSplitJavaCode_split16(int[] b) {

    TestSplitJavaCode_split9(b);

    TestSplitJavaCode_split10(b);

    TestSplitJavaCode_split11(b);

}

void TestSplitJavaCode_split17(int[] b) {
    TestSplitJavaCode_split12(b);

    TestSplitJavaCode_split13(b);

    TestSplitJavaCode_split14(b);

}


public void myFun1(int a) {
    rewrite$29[0] = false;
    myFun1_split22(a);

    myFun1_split21(a);
    if (rewrite$29[0]) { return; }










}
void myFun1_split18(int a) {

    this.rewrite$31[15] = a;
    rewrite$30[0][0] += rewrite$30[0][1];
    rewrite$30[0][1] += rewrite$30[0][2];
    rewrite$30[0][2] += rewrite$30[0][3];
    rewrite$31[10] = rewrite$30[0][0] + rewrite$30[0][1];
    rewrite$31[11] = rewrite$30[0][1] + rewrite$30[0][2];
}

void myFun1_split19(int a) {
    rewrite$31[12] = rewrite$30[0][2] + rewrite$30[0][0];
}

void myFun1_split20(int a) {
    for (int x : rewrite$30[0]) {rewrite$31[13] = x;
        System.out.println(rewrite$31[13] + rewrite$31[10] + rewrite$31[11] + rewrite$31[12]);
    }
}

void myFun1_split21(int a) {
    if (rewrite$31[10] > 0) {
        myFun1_0_0_rewriteGroup5(a);
    } else {
        rewrite$30[0][0] += 50;
        rewrite$30[0][1] += 100;
        rewrite$30[0][2] += 150;
        rewrite$30[0][3] += 200;
        rewrite$30[0][4] += 250;
        rewrite$30[0][5] += 300;
        rewrite$30[0][6] += 350;
        { rewrite$29[0] = true; return; }
    }
}

void myFun1_split22(int a) {

    myFun1_split18(a);

    myFun1_split19(a);

    myFun1_split20(a);

}


void myFun1_0_0_rewriteGroup5(int a) {
    myFun1_0_0_rewriteGroup5_split23(a);

    myFun1_0_0_rewriteGroup5_split24(a);

}
void myFun1_0_0_rewriteGroup5_split23(int a) {

    rewrite$30[0][0] += 100;
}

void myFun1_0_0_rewriteGroup5_split24(int a) {
    if (rewrite$31[11] > 0) {
        myFun1_0_0_rewriteGroup1_2_rewriteGroup4(a);
    } else {
        rewrite$30[0][1] += 50;
    }
}


void myFun1_0_0_rewriteGroup1_2_rewriteGroup4(int a) {
    myFun1_0_0_rewriteGroup1_2_rewriteGroup4_split25(a);

    myFun1_0_0_rewriteGroup1_2_rewriteGroup4_split26(a);

}
void myFun1_0_0_rewriteGroup1_2_rewriteGroup4_split25(int a) {

    rewrite$30[0][1] += 100;
}

void myFun1_0_0_rewriteGroup1_2_rewriteGroup4_split26(int a) {
    if (rewrite$31[12] > 0) {
        rewrite$30[0][2] += 100;
    } else {
        rewrite$30[0][2] += 50;
    }
}



public int myFun2(int[] a) { myFun2Impl(a); return rewrite$31[14]; }

void myFun2Impl(int[] a) {
    rewrite$29[1] = false;
    myFun2Impl_split27(a);

    myFun2Impl_split28(a);
    if (rewrite$29[1]) { return; }





}
void myFun2Impl_split27(int[] a) {

    a[0] += 1;
    a[1] += 2;
    a[2] += 3;
    a[3] += 4;
    a[4] += 5;
}

void myFun2Impl_split28(int[] a) {
    { rewrite$31[14] = a[0] + a[1] + a[2] + a[3] + a[4]; { rewrite$29[1] = true; return; } }
}

}
