import org.junit.Test;


public class Spatial2dJoinTest {

    @Test
    public void debug() throws Exception {
        String[] input = new String[3];

        

        input[0] = "file:///pathto/Point.txt";
        input[1] = "file:///pathto/Rectangle.txt";
        input[2] = "file:///pathto/output";

        Spatial2dJoin spatial2dJoin = new Spatial2dJoin();
        spatial2dJoin.main(input);
    }
}