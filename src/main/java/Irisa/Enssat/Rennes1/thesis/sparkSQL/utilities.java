package Irisa.Enssat.Rennes1.thesis.sparkSQL;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by letrungdung on 04/04/2018.
 */
public class utilities {
    public static String ListName(String homeSetTable, String logicalId){
        String folder = "data/dream/original/" +homeSetTable + "/" + logicalId;
        return folder;
    }
    public static void setupFile(String Masterfoler, String listCond) throws IOException {
        String folder = Masterfoler+ "/" + listCond;
        File file = new File(Masterfoler + "/" + "tree.txt");
        if (!file.exists()) {
            file.createNewFile();
        }
        //file.createNewFile();
        BufferedWriter writer = new BufferedWriter(new FileWriter(Masterfoler + "/" + "tree.txt", true));
        writer.write(listCond+"\n"); // for the new line
        writer.close();
    }
    public static void renewFile (String file) throws IOException {
        Path filePath = Paths.get(file);
        if (Files.exists(filePath))
            Files.delete(filePath);
        else Files.createFile(filePath);
    }
}
