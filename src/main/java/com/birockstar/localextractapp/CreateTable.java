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

import java.util.ArrayList;
import java.util.List;

// [START bigquery_create_table]
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class CreateTable {

  public static void main(String[] args) {

    String projectId = "YOUR_PROJECT_ID";

    String datasetName = "Staging";
    String[] tableNames = { "Invoices" };

    autoCreate(projectId, datasetName, tableNames);

  }

  public static void autoCreate(String projectId, String datasetName, String[] tableNames) {

    for (String tableName : tableNames) {

      Schema schema = BQStream.getSchema(datasetName, tableName);

      /*FieldList fl = schema.getFields();

      fl.forEach(a->System.out.println(a.getName() + " - " + a.getType()));*/

      // ingestion time partitioned
      CreatePartitionedTable.createPartitionedTable(projectId, datasetName, tableName, schema);

      //createTable(datasetName, tableName, schema);

    }

  }

  public static void semiAutoCreate(String projectId) {

    String[] tables = { "STOCK" , "StockLocation" };

    for (String tableName : tables) {

      Map<String, String> m = new LinkedHashMap<>();

      m = MSSQLSchema.getSQLSchema(tableName);

      String datasetName = "Bucket";

      List<Field> fields = new ArrayList<>();

      fields.add(Field.of("StoreID", StandardSQLTypeName.INT64));
      fields.add(Field.of("ExtractDateTime", StandardSQLTypeName.STRING));

      List<String> tableCol = new ArrayList<>(); /* = LoadCsvFromGcs.getADEcolumns(tableName); */

      for (String colName : tableCol) {

        try {

          // System.out.println("colName: " + colName);

          String colType = m.get(colName.trim());

          // System.out.println("colType: " + colType);

          switch (colType) {

            case "bit":
              fields.add(Field.of(colName, StandardSQLTypeName.BOOL));
              break;
            case "datetime":
              fields.add(Field.of(colName, StandardSQLTypeName.STRING));
              break;
            case "float":
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

        } catch (NullPointerException e) {
          System.out.println("Column not found...aborting...");
          return;
        }

      }

      Schema schema = Schema.of(fields);

      if (tableName.equals("EMPLOYEE")) {

        tableName = "EMPLOYEETNA";
      }

      // ingestion time partitioned
      CreatePartitionedTable.createPartitionedTable(projectId, datasetName, tableName, schema);

      // createTable(datasetName, tableName, schema);

    }

  }

  public static void putSchema(String projectId) {
    // TODO(developer): Replace these variables before running the sample.
    String datasetName = "Bucket";
    String tableName = "stktakeedit";

    List<Field> fields = new ArrayList<>();

    fields.add(Field.of("StoreID", StandardSQLTypeName.INT64));
    fields.add(Field.of("ExtractDateTime", StandardSQLTypeName.STRING));
    fields.add(Field.of("stktakedate", StandardSQLTypeName.STRING));
    fields.add(Field.of("EMPLOYEE", StandardSQLTypeName.STRING));

    Schema schema = Schema.of(fields);

    CreatePartitionedTable.createPartitionedTable(projectId, datasetName, tableName, schema);
  }

  public static void createTable(String datasetName, String tableName, Schema schema) {
    try {
      // Initialize client that will be used to send requests. This client only needs
      // to be created
      // once, and can be reused for multiple requests.
      BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

      TableId tableId = TableId.of(datasetName, tableName);
      TableDefinition tableDefinition = StandardTableDefinition.of(schema);
      TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

      bigquery.create(tableInfo);
      System.out.println("Table: " + tableName + " created successfully");
    } catch (BigQueryException e) {
      System.out.println("Table was not created. \n" + e.toString());
    }
  }
}