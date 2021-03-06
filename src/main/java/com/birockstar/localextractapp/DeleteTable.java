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

// [START bigquery_delete_table]
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;

public class DeleteTable {

  public static void main(String[] args) {

    String projectId = "YOUR_PROJECT_ID";

    String[] tables = { "Invoices" };

    for (String tableName : tables) {

      // TODO(developer): Replace these variables before running the sample.
      String datasetName = "Staging";
      // String tableName = "MY_TABLE_NAME";
      deleteTable(projectId, datasetName, tableName);
    }
  }

  public static void deleteTable(String projectId, String datasetName, String tableName) {
    try {
      // Initialize client that will be used to send requests. This client only needs
      // to be created
      // once, and can be reused for multiple requests.
      //BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
      
      BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService();

      boolean success = bigquery.delete(TableId.of(datasetName, tableName));
      if (success) {
        System.out.println("Table: " + tableName + " deleted successfully");
      } else {
        System.out.println("Table was not found");
      }
    } catch (BigQueryException e) {
      System.out.println("Table was not deleted. \n" + e.toString());
    }
  }
}
// [END bigquery_delete_table]
