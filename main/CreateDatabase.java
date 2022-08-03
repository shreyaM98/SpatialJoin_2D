
import java.io.IOException;
import java.util.*;
import java.lang.*;
import java.io.FileWriter;

public class CreateDatabase {

    public static void main(String [] args)
    {
        try {
            int minVal = 1;
            int maxVal = 10000;
            int maxHeight = 20;
            int maxWidth = 7;

            pointDataSet(minVal, maxVal);
            rectangleDataSet(minVal, maxVal, maxHeight, maxWidth);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void pointDataSet(int minVal, int maxVal) throws IOException {
        Random rand = new Random();
        FileWriter txtWriter = new FileWriter("Point.txt");
        for (int i = 0; i < 11000000; i++) {
            int x = rand.nextInt(maxVal - minVal + 1) + minVal;
            int y = rand.nextInt(maxVal - minVal + 1) + minVal;
            String p = String.valueOf(x) + "," + String.valueOf(y);
            txtWriter.write(String.valueOf(p)+"\n");
        }
        txtWriter.flush();
        txtWriter.close();
    }

    public static void rectangleDataSet(int minVal, int maxVal, int maxHeight, int maxWidth) throws IOException {
        Random rand = new Random();
        FileWriter txtWriter = new FileWriter("Rectangle.txt");
        for (int i = 0; i <= 5000000; i++) {
            int h = rand.nextInt(maxHeight - minVal + 1) + minVal;
            int w = rand.nextInt(maxWidth - minVal + 1) + minVal;
            int x = rand.nextInt((maxVal-maxWidth) - minVal + 1) + minVal;
            int y = rand.nextInt((maxVal-maxHeight) - minVal + 1) + minVal;

            String r = "r"+String.valueOf(i)+","+String.valueOf(x) + "," + String.valueOf(y)+","+ String.valueOf(h)+","+ String.valueOf(w);
            txtWriter.write(String.valueOf(r)+"\n");
        }
        txtWriter.flush();
        txtWriter.close();
    }
}

