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

import java.util.List;

// [START bigquery_create_table_partitioned]
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.common.collect.ImmutableList;

// Sample to create a partition table
public class CreatePartitionedTable {

  public static void main(String[] args) {

    String projectId = "YOUR_PROJECT_ID";

    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

    String datasetName = "YOUR_DATASET_NAME";

    String tableName = "YOUT_TABLE_NAME";

    TableId tableId = TableId.of(datasetName, tableName);

    Schema schema = bigquery.getTable(tableId).getDefinition().getSchema();

    createPartitionedTable(projectId, datasetName, tableName, schema, ImmutableList.of("StoreID","SupplierID", "KeyCode"), "GRVDate");


  }

  public static void oldMain() {
    // TODO(developer): Replace these variables before running the sample.
    String projectId = "YOUR_PROJECT_ID";
    String datasetName = "YOUR_DATASET_NAME";
    String tableName = "YOUR_TABLE_NAME";
    Schema schema = Schema.of(Field.of("name", StandardSQLTypeName.STRING),
        Field.of("post_abbr", StandardSQLTypeName.STRING), Field.of("date", StandardSQLTypeName.DATE));
    createPartitionedTable(projectId, datasetName, tableName, schema, ImmutableList.of("name", "post_abbr"), "GRVDate");
  }

  public static void createPartitionedTable(String projectId, String datasetName, String tableName, Schema schema,
      List<String> clusteringFields, String partitioningField) {
    try {
      // Initialize client that will be used to send requests. This client only needs
      // to be created
      // once, and can be reused for multiple requests.
      //BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

      BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService();

      TableId tableId = TableId.of(datasetName, tableName);

      TimePartitioning partitioning = TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
          .setField(partitioningField) // name of column to use for partitioning
          // .setExpirationMs(7776000000L) // 90 days
          .build();

      Clustering clustering = Clustering.newBuilder().setFields(clusteringFields).build();

      StandardTableDefinition tableDefinition = StandardTableDefinition.newBuilder().setSchema(schema)
          .setTimePartitioning(partitioning).setClustering(clustering).build();
      TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

      bigquery.create(tableInfo);
      System.out.println("Partitioned table: " + tableName + " created successfully");
    } catch (BigQueryException e) {
      System.out.println("Partitioned table: " + tableName + "  was not created. \n" + e.toString());
    }
  }

  public static void createPartitionedTable(String projectId, String datasetName, String tableName, Schema schema) {
    try {
      // Initialize client that will be used to send requests. This client only needs
      // to be created
      // once, and can be reused for multiple requests.
      //BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

      BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService();

      TableId tableId = TableId.of(datasetName, tableName);

      TimePartitioning partitioning = TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
          //.setField(partitioningField) // name of column to use for partitioning
          // .setExpirationMs(7776000000L) // 90 days
          .build();

      StandardTableDefinition tableDefinition = StandardTableDefinition.newBuilder().setSchema(schema)
          .setTimePartitioning(partitioning).build();
      TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

      bigquery.create(tableInfo);
      System.out.println("Partitioned table: " + tableName + " created successfully");
    } catch (BigQueryException e) {
      System.out.println("Partitioned table: " + tableName + "  was not created. \n" + e.toString());
    }
  }

}
// [END bigquery_create_table_partitioned]
