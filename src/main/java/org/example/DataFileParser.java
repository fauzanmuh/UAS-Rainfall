package org.example;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * A class represent data file parser
 */
public class DataFileParser {

    public static void main(String[] args) {

        //check if the args[0] is not null or empty
        if (args[0] == null || args[0].isEmpty()) {
            System.out.println("Invalid file name or file location");
            return;
        }
        try {
            //read all the lines of the file
            List<String> lines = Files.readAllLines(Paths.get(args[0]));
            lines.forEach(line -> {
                //it's a csv file, split into tokens using ","
                String[] tokens = line.split(",");
                //string builder in order to build a new line with new column of year average
                StringBuilder stringBuilder = new StringBuilder(tokens[0]);
                int sum = 0;
                //aggregate year values
                for (int i = 1; i < tokens.length; i++) {
                    stringBuilder.append(" ").append(tokens[i]);
                    sum += Integer.parseInt(tokens[i]);
                }
                //calc the year average
                int avg = sum / (tokens.length - 1);
                stringBuilder.append(" ").append(avg);
                //write the line into the new data file
                try (FileWriter writer = new FileWriter("ParsedDataFile.txt", true);
                     BufferedWriter bw = new BufferedWriter(writer))
                {
                    bw.write(stringBuilder.append("\n").toString());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("File created successfully");
    }
}