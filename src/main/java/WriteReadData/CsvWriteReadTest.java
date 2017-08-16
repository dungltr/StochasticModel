package WriteReadData;

import java.io.IOException;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author letrung
 */
public class CsvWriteReadTest {
    public void main() throws IOException {
		
		String fileName = System.getProperty("user.home")+"/student.csv";
		
		System.out.println("Write CSV file:");
//		CsvFileWriter.writeCsvFile(fileName);
//		Student student = new Student(10, "LE", "Trung", "M", 35);
//                CsvFileWriter.adddataCsv(fileName, student);
                
		System.out.println("\nRead CSV file:");
//		CsvFileReader.readCsvFile(fileName);
                
                System.out.println("\nLast line CSV file:");
		System.out.println(CsvFileReader.tail(fileName));
                
                System.out.println("\nLast 2 line CSV file:");
		System.out.println(CsvFileReader.tail2(fileName,2));
                
                

	}
}
