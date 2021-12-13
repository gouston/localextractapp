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
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
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

public class LocalQueries {

    public static void main(String[] args) {

        // File Name must equal the BigQuery table Name. And the subFolder must equal
        // the dataset name

        String projectId = "YOUR_PROJECT_ID";

        String localDirectory = "D:\\Documents\\VS Code\\localextractapp\\queries";

        iterateSubFolders(projectId, localDirectory);

    }

    public static void iterateSubFolders(String projectId, String localDirectory) {

        Path p1 = null;

        try {
            p1 = Paths.get(localDirectory);
            if (!p1.toFile().exists()) {
                // doesn't exist
                // create?

                System.out.println("Directory doesn't exist! Exiting...");
                return;

            }

        } catch (InvalidPathException e) {
            System.out.println("Invalid Path ie cannot be parsed...");

        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(p1)) {
            for (Path file : stream) {
                // System.out.println(file.getFileName());

                iterateQueries(projectId, localDirectory, file.getFileName().toFile().getName());

            }

        } catch (IOException | DirectoryIteratorException x) {
            // IOException can never be thrown by the iteration.
            // In this snippet, it can only be thrown by newDirectoryStream.
            System.err.println(x);
        }

        // p1.forEach(a->System.out.println(a.getFileName()));

    }

    public static void iterateQueries(String projectId, String localDirectory, String subFolderName) {

        // System.out.println("localDirectory is: " + localDirectory + " and subFolder
        // is: " + subFolder);

        String subFolderDirectory = localDirectory + "\\" + subFolderName;

        System.out.println("subFolderDirectory is: " + subFolderDirectory);

        Path subFolderPath = Paths.get(subFolderDirectory);

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(subFolderPath)) {
            for (Path file : stream) {

                // System.out.println(file.getFileName());
                // TODO: run each query sending the results to BigQuery
                // read the file contents, it should contain a valid query. Run this query.

                // FileOperations.readQuery(file);

                // File Name must equal the BigQuery table Name. And the subFolder must equal
                // the dataset name

                String datasetName = subFolderName;
                String tableName = file.toFile().getName().replaceAll(".sql", "");
                String query = FileOperations.returnQuery(file);

                /*System.out.printf("%s%n",
                        "datasetName is: " + datasetName + " tableName is: " + tableName + " query is: " + query);
                System.out.println("\n");*/

                BQStream.streamLocalToBQ(projectId, datasetName, tableName, query);

            }

        } catch (IOException | DirectoryIteratorException x) {
            // IOException can never be thrown by the iteration.
            // In this snippet, it can only be thrown by newDirectoryStream.
            System.err.println(x);
        }

    }

}
