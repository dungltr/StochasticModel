/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package WriteReadData;

import Algorithms.testScilab;
import Algorithms.testWriteMatrix2CSV;
import java.io.IOException;

/**
 *
 * @author letrung
 */
public class testWriteReadCSV {
    //        @Test
        public void testWriteReadTestCsv() throws IOException {
            CsvWriteReadTest test = new CsvWriteReadTest();
            test.main();
        }
//        @Test
        public void testWriteMatrixCsv() throws IOException {
            testWriteMatrix2CSV.main();
        }
//        @Test
        public void testAlgorim() throws IOException {
            testScilab.testScilab();
        }
    
}
