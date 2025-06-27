public class TestSplitJavaCode {
boolean[] rewrite$26 = new boolean[2];
int[][] rewrite$27 = new int[1][];
int[] rewrite$28 = new int[16];

{
rewrite$28[15] = 1;
}























public TestSplitJavaCode(int[] b) {
TestSplitJavaCode_split9(b);

TestSplitJavaCode_split10(b);

TestSplitJavaCode_split11(b);

TestSplitJavaCode_split12(b);

TestSplitJavaCode_split13(b);

TestSplitJavaCode_split14(b);

TestSplitJavaCode_split15(b);















}
void TestSplitJavaCode_split9(int[] b) {

this.rewrite$27[0] = b;
rewrite$28[0] = this.rewrite$27[0][0] + b[0];
System.out.println("b0 = " + rewrite$28[0]);
rewrite$28[1] = this.rewrite$27[0][1] + b[1];
}

void TestSplitJavaCode_split10(int[] b) {
System.out.println("b1 = " + rewrite$28[1]);
rewrite$28[2] = this.rewrite$27[0][2] + b[2];
System.out.println("b2 = " + rewrite$28[2]);
}

void TestSplitJavaCode_split11(int[] b) {
rewrite$28[3] = this.rewrite$27[0][3] + b[3];
System.out.println("b3 = " + rewrite$28[3]);
rewrite$28[4] = this.rewrite$27[0][4] + b[4];
}

void TestSplitJavaCode_split12(int[] b) {
System.out.println("b4 = " + rewrite$28[4]);
rewrite$28[5] = this.rewrite$27[0][5] + b[5];
System.out.println("b5 = " + rewrite$28[5]);
}

void TestSplitJavaCode_split13(int[] b) {
rewrite$28[6] = this.rewrite$27[0][6] + b[6];
System.out.println("b6 = " + rewrite$28[6]);
rewrite$28[7] = this.rewrite$27[0][7] + b[7];
}

void TestSplitJavaCode_split14(int[] b) {
System.out.println("b7 = " + rewrite$28[7]);
rewrite$28[8] = this.rewrite$27[0][8] + b[8];
System.out.println("b8 = " + rewrite$28[8]);
}

void TestSplitJavaCode_split15(int[] b) {
rewrite$28[9] = this.rewrite$27[0][9] + b[9];
System.out.println("b9 = " + rewrite$28[9]);
}


public void myFun1(int a) {
rewrite$26[0] = false;
myFun1_split16(a);

myFun1_split17(a);

myFun1_split18(a);

myFun1_split19(a);
if (rewrite$26[0]) { return; }








}
void myFun1_split16(int a) {

this.rewrite$28[15] = a;
rewrite$27[0][0] += rewrite$27[0][1];
rewrite$27[0][1] += rewrite$27[0][2];
rewrite$27[0][2] += rewrite$27[0][3];
rewrite$28[10] = rewrite$27[0][0] + rewrite$27[0][1];
rewrite$28[11] = rewrite$27[0][1] + rewrite$27[0][2];
}

void myFun1_split17(int a) {
rewrite$28[12] = rewrite$27[0][2] + rewrite$27[0][0];
}

void myFun1_split18(int a) {
for (int x : rewrite$27[0]) {rewrite$28[13] = x;
System.out.println(rewrite$28[13] + rewrite$28[10] + rewrite$28[11] + rewrite$28[12]);
}
}

void myFun1_split19(int a) {
if (rewrite$28[10] > 0) {
myFun1_0_0_rewriteGroup5(a);
} else {
rewrite$27[0][0] += 50;
rewrite$27[0][1] += 100;
rewrite$27[0][2] += 150;
rewrite$27[0][3] += 200;
rewrite$27[0][4] += 250;
rewrite$27[0][5] += 300;
rewrite$27[0][6] += 350;
{ rewrite$26[0] = true; return; }
}
}


void myFun1_0_0_rewriteGroup5(int a) {
myFun1_0_0_rewriteGroup5_split20(a);

myFun1_0_0_rewriteGroup5_split21(a);

}
void myFun1_0_0_rewriteGroup5_split20(int a) {

rewrite$27[0][0] += 100;
}

void myFun1_0_0_rewriteGroup5_split21(int a) {
if (rewrite$28[11] > 0) {
myFun1_0_0_rewriteGroup1_2_rewriteGroup4(a);
} else {
rewrite$27[0][1] += 50;
}
}


void myFun1_0_0_rewriteGroup1_2_rewriteGroup4(int a) {
myFun1_0_0_rewriteGroup1_2_rewriteGroup4_split22(a);

myFun1_0_0_rewriteGroup1_2_rewriteGroup4_split23(a);

}
void myFun1_0_0_rewriteGroup1_2_rewriteGroup4_split22(int a) {

rewrite$27[0][1] += 100;
}

void myFun1_0_0_rewriteGroup1_2_rewriteGroup4_split23(int a) {
if (rewrite$28[12] > 0) {
rewrite$27[0][2] += 100;
} else {
rewrite$27[0][2] += 50;
}
}



public int myFun2(int[] a) { myFun2Impl(a); return rewrite$28[14]; }

void myFun2Impl(int[] a) {
rewrite$26[1] = false;
myFun2Impl_split24(a);

myFun2Impl_split25(a);
if (rewrite$26[1]) { return; }





}
void myFun2Impl_split24(int[] a) {

a[0] += 1;
a[1] += 2;
a[2] += 3;
a[3] += 4;
a[4] += 5;
}

void myFun2Impl_split25(int[] a) {
{ rewrite$28[14] = a[0] + a[1] + a[2] + a[3] + a[4]; { rewrite$26[1] = true; return; } }
}

}
