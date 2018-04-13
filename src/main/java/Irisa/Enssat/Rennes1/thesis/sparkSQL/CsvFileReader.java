/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Irisa.Enssat.Rennes1.thesis.sparkSQL;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
/**
 *
 * @author letrung
 */
public class CsvFileReader {
    //Delimiter used in CSV file
	private static final String COMMA_DELIMITER = ",";
    private static final String NEW_LINE_SEPARATOR = "\n";
    public static int count(String filename) throws IOException {
        InputStream is = new BufferedInputStream(new FileInputStream(filename));
        try {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1) {
            empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                    ++count;
                    }
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        } finally {
    is.close();
    }
    }
}
