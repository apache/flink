public class TestLocalVariableWithSameName {
    public void myFun1() {
        int local1;
        String local2 = "AAAAA";
        final long local3;
        local3 = 100;
    }

    public void myFun2() {
        int local1;
        local1 = 5;
        long local2;
        final String local3 = "BBBBB";
    }
}
