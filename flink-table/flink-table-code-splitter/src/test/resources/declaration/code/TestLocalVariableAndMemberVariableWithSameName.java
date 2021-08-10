public class TestLocalVariableAndMemberVariableWithSameName {
    int local1;
    String local2;
    long local3 = 100L;

    public void myFun() {
        int local1;
        String local2 = "local2";
        final long local3;
        local3 = 100L;
    }
}
