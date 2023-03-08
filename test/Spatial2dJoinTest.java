import org.junit.Test;

import static org.junit.Assert.*;

public class Spatial2dJoinTest {

    @Test
    public void debug() throws Exception{

        String[] input = new String[4];
        input[0] = "file:///IdeaProjects/SpatialJoin/input/Point.txt";
        input[1] = "file:///IdeaProjects/SpatialJoin/input/Rectangle.txt";
        input[2] = "file:///IdeaProjects/SpatialJoin/output/outputFileSpatial";
        input[3] = "250,250,750,750";

        Spatial2dJoin spatial2dJoin = new Spatial2dJoin();
        spatial2dJoin.debugSpatial2dJoin(input);
    }
}