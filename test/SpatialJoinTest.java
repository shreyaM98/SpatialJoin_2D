import org.junit.Test;


public class SpatialJoinTest {

    @Test
    public void debug() throws Exception {
        String[] input = new String[3];

        

        input[0] = "file:///pathto/Point.txt";
        input[1] = "file:///pathto/Rectangle.txt";
        input[2] = "file:///pathto/output";

        SpatialJoin spatialJoin = new SpatialJoin();
        spatialJoin.main(input);
    }
}