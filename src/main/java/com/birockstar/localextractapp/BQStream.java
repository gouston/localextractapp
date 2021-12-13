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

// [START bigquery_inserting_data_types]
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.google.auth.oauth2.GoogleCredentials;
import java.io.FileInputStream;
import java.io.IOException;

// Sample to insert data types in a table
public class BQStream {


  //TODO: update database, user and password
  private static final String CONNECTIONURL = "jdbc:sqlserver://localhost:1433;databaseName=YOUR_DATABASE_NAME;user=YOUR_USER_NAME;password=YOUR_PASSWORD";

  public static void main(String[] args) {

    String projectId = "YOUR_PROJECT_ID";

    streamLocalToBQ(projectId);

  }

  public static void streamLocalToBQ(String projectId, String datasetName, String tableName, String query) {

    //TODO: update the path to your JSON credential file
    String jsonPath = "D:\\Downloads\\YOUR_JSON_CREDENTIAL_FILE.json";

    ArrayList<String> scopes = new ArrayList<String>();

    scopes.add("https://www.googleapis.com/auth/bigquery");
    scopes.add("https://www.googleapis.com/auth/bigquery.readonly");

    GoogleCredentials credentials;
    try {
      /*
       * credentials = GoogleCredentials.getApplicationDefault(); credentials =
       * credentials.createScoped(scopes); credentials.refreshAccessToken();
       */

      credentials = GoogleCredentials.fromStream(new FileInputStream(jsonPath)).createScoped(scopes);

      credentials.refreshAccessToken();

      /*
       * String accessToken = credentials.refreshAccessToken().getTokenValue();
       * 
       * if (accessToken == null) { System.out.println("accessToken is null"); return;
       * }
       */

      // BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

      // Initialize client that will be used to send requests. This client only needs
      // to be created
      // once, and can be reused for multiple requests.

      BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).setCredentials(credentials)
          .build().getService();

      TableId tableId = TableId.of(datasetName, tableName);
      getlocalSQLdata(bigquery, tableId, datasetName, tableName, query);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

  public static void getlocalSQLdata(BigQuery bigquery, TableId tableId, String datasetName, String tableName,
      String query) {

    // StringBuilder sb = new StringBuilder();

    int batchSize = 500;
    int i = 1;
    int j = 1;

    Map<String, String> m = new LinkedHashMap<>();

    m = MSSQLSchema.getSQLSchema(tableName);

    List<InsertAllRequest.RowToInsert> listRows = new ArrayList<>();

    try (Connection connection = DriverManager.getConnection(CONNECTIONURL)) {

      try (PreparedStatement statement = connection.prepareStatement(query)) {

        ResultSet rs = statement.executeQuery();

        // while there are rows to process from the source query

        System.out.println("Starting processing of datasetName: " + datasetName + " tableName: " + tableName);

        while (rs.next()) {

          // System.out.println("starting processing of row: " + i);

          // if current row falls outside of the current batch, stream the full batch and
          // then make a new batch
          if (i > batchSize * j) {

            // when the batch is full, stream it
            // System.out.println("row: " + i + " is outsidebatch: " + j + "! It's full, so
            // time to stream it!");
            streamAllRows(bigquery, tableId, listRows);

            // clear the list for the next batch
            listRows.clear();

            if (listRows.size() != 0) {
              System.out.println("batch: " + j + ": listRows has not been cleared...");
            }
            j++;
            System.out.println("Starting new batch: " + j);
            // increment batch counter

          }

          // add rows into same batch
          // if (i <= (batchSize * j)) {

          // build current row content
          Map<String, Object> rowContent = new HashMap<>();
          for (Map.Entry<String, String> e : m.entrySet()) {

            String colName = e.getKey();
            rowContent.put(colName, rs.getString(colName));

          }

          // add current row to list
          listRows.add(InsertAllRequest.RowToInsert.of(rowContent));

          // }

          // System.out.println("finished processing row: " + i);

          // increment row counter
          i++;

        }

        // stream remaining batch

        if (listRows.size() != 0) {
          System.out.println("Streaming final batch: " + j);
          streamAllRows(bigquery, tableId, listRows);
        }

      }

    }

    catch (SQLException e) {

      e.printStackTrace();
    }

  }

  public static void streamLocalToBQ(String projectId) {

    //TODO: update the path to your JSON credential file
    String jsonPath = "D:\\Downloads\\YOUR_JSON_CREDENTIAL_FILE.json";

    ArrayList<String> scopes = new ArrayList<String>();

    scopes.add("https://www.googleapis.com/auth/bigquery");
    scopes.add("https://www.googleapis.com/auth/bigquery.readonly");

    GoogleCredentials credentials;
    try {
      /*
       * credentials = GoogleCredentials.getApplicationDefault(); credentials =
       * credentials.createScoped(scopes); credentials.refreshAccessToken();
       */

      credentials = GoogleCredentials.fromStream(new FileInputStream(jsonPath)).createScoped(scopes);

      credentials.refreshAccessToken();

      /*
       * String accessToken = credentials.refreshAccessToken().getTokenValue();
       * 
       * if (accessToken == null) { System.out.println("accessToken is null"); return;
       * }
       */

      String datasetName = "Staging";
      String tableName = "OrderLines";

      // BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

      // Initialize client that will be used to send requests. This client only needs
      // to be created
      // once, and can be reused for multiple requests.

      BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).setCredentials(credentials)
          .build().getService();

      TableId tableId = TableId.of(datasetName, tableName);
      getlocalSQLdata(bigquery, tableId, datasetName, tableName);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

  public static void getlocalSQLdata(BigQuery bigquery, TableId tableId, String datasetName, String tableName) {

    // StringBuilder sb = new StringBuilder();

    int batchSize = 500;
    int i = 1;
    int j = 1;

    String query = "SELECT TOP (240000) * \n" + "FROM [WideWorldImporters].[Sales].[OrderLines]";

    Map<String, String> m = new LinkedHashMap<>();

    m = MSSQLSchema.getSQLSchema(tableName);

    List<InsertAllRequest.RowToInsert> listRows = new ArrayList<>();

    try (Connection connection = DriverManager.getConnection(CONNECTIONURL)) {

      try (PreparedStatement statement = connection.prepareStatement(query)) {

        ResultSet rs = statement.executeQuery();

        // while there are rows to process from the source query

        System.out.println("Starting processing...");

        while (rs.next()) {

          // System.out.println("starting processing of row: " + i);

          // if current row falls outside of the current batch, stream the full batch and
          // then make a new batch
          if (i > batchSize * j) {

            // when the batch is full, stream it
            // System.out.println("row: " + i + " is outsidebatch: " + j + "! It's full, so
            // time to stream it!");
            streamAllRows(bigquery, tableId, listRows);

            // clear the list for the next batch
            listRows.clear();

            if (listRows.size() != 0) {
              System.out.println("batch: " + j + ": listRows has not been cleared...");
            }
            j++;
            System.out.println("Starting new batch: " + j);
            // increment batch counter

          }

          // add rows into same batch
          // if (i <= (batchSize * j)) {

          // build current row content
          Map<String, Object> rowContent = new HashMap<>();
          for (Map.Entry<String, String> e : m.entrySet()) {

            String colName = e.getKey();
            rowContent.put(colName, rs.getString(colName));

          }

          // add current row to list
          listRows.add(InsertAllRequest.RowToInsert.of(rowContent));

          // }

          // System.out.println("finished processing row: " + i);

          // increment row counter
          i++;

        }

        // stream remaining batch

        if (listRows.size() != 0) {
          System.out.println("Streaming final batch: " + j);
          streamAllRows(bigquery, tableId, listRows);
        }

      }

    }

    catch (SQLException e) {

      e.printStackTrace();
    }

  }

  public static void streamAllRows(BigQuery bigquery, TableId tableId, List<InsertAllRequest.RowToInsert> allRows) {

    try {

      // InsertAllResponse response =
      // bigquery.insertAll(InsertAllRequest.newBuilder(tableId).addRow(rowContent).build());

      InsertAllResponse response = bigquery.insertAll(InsertAllRequest.newBuilder(tableId).setRows(allRows).build());

      // InsertAllResponse response = bigquery.insertAll(InsertAllRequest.of(tableId,
      // allRows));

      if (response.hasErrors()) {
        // If any of the insertions failed, this lets you inspect the errors
        for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
          System.out.println("Response error: \n" + entry.getValue());
        }
      }
      System.out.println("Rows successfully inserted into table");
    } catch (BigQueryException e) {
      System.out.println("Insert operation not performed \n" + e.toString());
    }
  }

  public static void streamingInsert(BigQuery bigquery, TableId tableId, Map<String, Object> rowContent) {
    try {

      InsertAllResponse response = bigquery.insertAll(InsertAllRequest.newBuilder(tableId).addRow(rowContent).build());

      if (response.hasErrors()) {
        // If any of the insertions failed, this lets you inspect the errors
        for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
          System.out.println("Response error: \n" + entry.getValue());
        }
      }
      System.out.println("Rows successfully inserted into table");
    } catch (BigQueryException e) {
      System.out.println("Insert operation not performed \n" + e.toString());
    }
  }

  public static Schema getSchema(String datasetName, String tableName) {

    // System.out.println("getting schema");

    Map<String, String> m = new LinkedHashMap<>();

    m = MSSQLSchema.getSQLSchema(tableName);

    List<Field> fields = new ArrayList<>();

    for (Map.Entry<String, String> e : m.entrySet()) {

      String colName = e.getKey();

      // System.out.println("colName is: " + colName);

      switch (e.getValue()) {

        case "bit":
          fields.add(Field.of(colName, StandardSQLTypeName.INT64));
          break;
        case "date":
          fields.add(Field.of(colName, StandardSQLTypeName.STRING));
          break;
        case "datetime":
          fields.add(Field.of(colName, StandardSQLTypeName.STRING));
          break;
        case "datetime2":
          fields.add(Field.of(colName, StandardSQLTypeName.STRING));
          break;
        case "float":
          fields.add(Field.of(colName, StandardSQLTypeName.FLOAT64));
          break;
        case "decimal":
          fields.add(Field.of(colName, StandardSQLTypeName.FLOAT64));
          break;
        case "int":
          fields.add(Field.of(colName, StandardSQLTypeName.INT64));
          break;
        case "money":
          fields.add(Field.of(colName, StandardSQLTypeName.FLOAT64));
          break;
        case "nvarchar":
          fields.add(Field.of(colName, StandardSQLTypeName.STRING));
          break;
        case "varchar":
          fields.add(Field.of(colName, StandardSQLTypeName.STRING));
          break;

      }

    }

    // fields.stream().forEach(a->System.out.println(a.getName()));

    Schema schema = Schema.of(fields);

    return schema;

  }

  public static void insertingDataTypes(String datasetName, String tableName) {
    try {
      // Initialize client that will be used to send requests. This client only needs
      // to be created
      // once, and can be reused for multiple requests.
      BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

      // Inserting data types
      Field name = Field.of("name", StandardSQLTypeName.STRING);
      Field age = Field.of("age", StandardSQLTypeName.INT64);
      Field school = Field.newBuilder("school", StandardSQLTypeName.BYTES).setMode(Field.Mode.REPEATED).build();
      Field location = Field.of("location", StandardSQLTypeName.GEOGRAPHY);
      Field measurements = Field.newBuilder("measurements", StandardSQLTypeName.FLOAT64).setMode(Field.Mode.REPEATED)
          .build();
      Field day = Field.of("day", StandardSQLTypeName.DATE);
      Field firstTime = Field.of("firstTime", StandardSQLTypeName.DATETIME);
      Field secondTime = Field.of("secondTime", StandardSQLTypeName.TIME);
      Field thirdTime = Field.of("thirdTime", StandardSQLTypeName.TIMESTAMP);
      Field datesTime = Field.of("datesTime", StandardSQLTypeName.STRUCT, day, firstTime, secondTime, thirdTime);
      Schema schema = Schema.of(name, age, school, location, measurements, datesTime);

      TableId tableId = TableId.of(datasetName, tableName);
      TableDefinition tableDefinition = StandardTableDefinition.of(schema);
      TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

      bigquery.create(tableInfo);

      // Inserting Sample data
      Map<String, Object> datesTimeContent = new HashMap<>();
      datesTimeContent.put("day", "2019-1-12");
      datesTimeContent.put("firstTime", "2019-02-17 11:24:00.000");
      datesTimeContent.put("secondTime", "14:00:00");
      datesTimeContent.put("thirdTime", "2020-04-27T18:07:25.356Z");

      Map<String, Object> rowContent = new HashMap<>();
      rowContent.put("name", "Tom");
      rowContent.put("age", 30);
      rowContent.put("school", "Test University".getBytes());
      rowContent.put("location", "POINT(1 2)");
      rowContent.put("measurements", new Float[] { 50.05f, 100.5f });
      rowContent.put("datesTime", datesTimeContent);

      InsertAllResponse response = bigquery.insertAll(InsertAllRequest.newBuilder(tableId).addRow(rowContent).build());

      if (response.hasErrors()) {
        // If any of the insertions failed, this lets you inspect the errors
        for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
          System.out.println("Response error: \n" + entry.getValue());
        }
      }
      System.out.println("Rows successfully inserted into table");
    } catch (BigQueryException e) {
      System.out.println("Insert operation not performed \n" + e.toString());
    }
  }
}
// [END bigquery_inserting_data_types]
