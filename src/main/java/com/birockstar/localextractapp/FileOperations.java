
/*
 * Copyright 2021 Michael Houston
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.birockstar.localextractapp;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.NoSuchFileException;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import java.sql.Timestamp;
import java.time.Instant;

public class FileOperations {

    public static void main(String[] args) {

        /*
         * String uuid = UUID.randomUUID().toString();
         * 
         * System.out.println(uuid);
         */

        // String localFile = "29c0dbd7-84e3-4f91-94ef-67d1dba1aaf8";

        //System.out.println(readTimesChecked());

        //writeTimesChecked(7);

        /*try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }*/

        System.out.println(readTimesChecked());

        // deleteFile();

    }

    public static void readQuery(Path queryFile) {

        Charset charset = Charset.forName("US-ASCII");
        try (BufferedReader reader = Files.newBufferedReader(queryFile, charset)) {
            String line = null;
            while ((line = reader.readLine()) != null) {

                System.out.println(line);

            }
        } catch (IOException x) {
            System.err.format("1. IOException: %s%n", x);
        }

    }

    public static String returnQuery(Path queryFile) {

        StringBuilder sb = new StringBuilder();

        Charset charset = Charset.forName("US-ASCII");
        try (BufferedReader reader = Files.newBufferedReader(queryFile, charset)) {
            String line = null;
            while ((line = reader.readLine()) != null) {

                //System.out.println(line);
                sb.append(line);
            }
        } catch (IOException x) {
            System.err.format("1. IOException: %s%n", x);
        }

        return sb.toString();

    }

    public static int readTimesChecked() {

        String localFile = "D:\\Downloads\\29c0dbd7-84e3-4f91-94ef-67d1dba1aaf8";

        Path p1 = null;

        try {
            p1 = Paths.get(localFile);
            if (!p1.toFile().exists()) {

                writeTimesChecked(0);

            }

        } catch (InvalidPathException e) {
            System.out.println("Path cannot be parsed...");

        }

        int timesChecked = 0;

        Charset charset = Charset.forName("US-ASCII");
        try (BufferedReader reader = Files.newBufferedReader(p1, charset)) {
            String line = null;
            while ((line = reader.readLine()) != null) {

                timesChecked = Integer.valueOf(line.trim());

            }
        } catch (IOException x) {
            System.err.format("IOException: %s%n", x);
        }

        return timesChecked;
    }

    public static void deleteFile() {

        String fileToWrite = "D:\\Downloads\\29c0dbd7-84e3-4f91-94ef-67d1dba1aaf8";

        File outputFile = new File(fileToWrite);

        if (outputFile.delete()) {

            System.out.println("file was deleted");
        }

    }

    public static void writeTimesChecked(int timesChecked) {

        boolean exists = false;

        String fileToWrite = "D:\\Downloads\\29c0dbd7-84e3-4f91-94ef-67d1dba1aaf8";

        try {

            Path p1 = Paths.get(fileToWrite);
            exists = p1.toFile().exists();

        } catch (InvalidPathException e) {
            System.out.println("Path cannot be parsed");
        }

        if (!exists) {

            File outputFile = new File(fileToWrite);
            System.out.println("File to create: " + outputFile.getName());

            try {
                // create the file
                if (outputFile.createNewFile()) {
                    System.out.println("File: " + fileToWrite + " created!");
                    // create the file writer
                } else {
                    System.out.println("File not created...");
                }
            } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }

        FileWriter myWriter = null;

        try {

            myWriter = new FileWriter(fileToWrite);

            myWriter.write(String.valueOf(timesChecked));

            System.out.println("timesChecked written!");

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        try {
            myWriter.close();
            System.out.println("closing writer...");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}