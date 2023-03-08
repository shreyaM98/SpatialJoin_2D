import java.util.ArrayList;
import java.util.List;

public class DivideIntoBlocks {
    /**
     * Create a list of grid blocks.
     * @param maxPoint The maximum point of the two-dimensional space.
     * @param gridSize The size of each grid block.
     * @return A list of strings, where each string represents a grid block in the format "gX,Y,X1,Y1,X2,Y2".
     */
    public static List<String> createBlocks(Integer maxPoint, Integer gridSize) {
        // Create an empty list to hold the grid blocks.
        List<String> grids = new ArrayList<>();

        // Create a StringBuilder object to construct each grid block string.
        StringBuilder grid = new StringBuilder();
        int gridCount = 1;

        // Loop through the two-dimensional space and create a grid block for each grid cell.
        for (int i = 0; i < maxPoint; i += gridSize) {
            for (int j = 0; j < maxPoint; j += gridSize) {
                
                grid.setLength(0); // Clear the StringBuilder and append the grid block string in the format "gX,Y,X1,Y1,X2,Y2".
                grid.append("g").append(gridCount).append(",")
                        .append(i).append(",").append(j).append(",")
                        .append(i + gridSize).append(",").append(j + gridSize);
                // Add the grid block string to the list of grids.
                grids.add(grid.toString());
                gridCount++;
            }
        }
        return grids;
    }

}
