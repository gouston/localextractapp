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

import java.util.UUID;

import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.ExtractJobConfiguration;
import com.google.cloud.bigquery.ExtractJobConfiguration.Builder;
import org.threeten.bp.Duration;

public class ExtractTableToCsv {

    public static void runExtractTableToCsv() {
        // TODO(developer): Replace these variables before running the sample.
        String projectId = "YOUR_PROJECT_ID";
        String datasetName = "YOUR_DATASET_NAME";
        String tableName = "shakespeare";
        String bucketName = "my-bucket";
        String destinationUri = "gs://" + bucketName + "/path/to/file";
        // For more information on export formats available see:
        // https://cloud.google.com/bigquery/docs/exporting-data#export_formats_and_compression_types
        // For more information on Job see:
        // https://googleapis.dev/java/google-cloud-clients/latest/index.html?com/google/cloud/bigquery/package-summary.html
    
        String dataFormat = "CSV";
        extractTableToCsv(projectId, datasetName, tableName, destinationUri, dataFormat);
      }
    
      public static void extractTableToCsvCustom(
          String projectId,
          String datasetName,
          String tableName,
          String destinationUri,
          String dataFormat) {
        try {
          // Initialize client that will be used to send requests. This client only needs to be created
          // once, and can be reused for multiple requests.
          BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    
          TableId tableId = TableId.of(projectId, datasetName, tableName);
          //Table table = bigquery.getTable(tableId);
    
          ExtractJobConfiguration extjobconfig = ExtractJobConfiguration.newBuilder(tableId, destinationUri)
          .setCompression("GZIP")
          .setFormat(dataFormat)
          .setPrintHeader(true)
          .setFieldDelimiter("|")
          .build();

          JobId jobId = JobId.of(UUID.randomUUID().toString());
          Job job = bigquery.create(JobInfo.newBuilder(extjobconfig).setJobId(jobId).build());

          //Job job = table.extract(dataFormat, destinationUri).toBuilder().setConfiguration(extjobconfig).build();
    
          // Blocks until this job completes its execution, either failing or succeeding.
          Job completedJob =
              job.waitFor(
                  RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
                  RetryOption.totalTimeout(Duration.ofMinutes(3)));
          if (completedJob == null) {
            System.out.println("Job not executed since it no longer exists.");
            return;
          } else if (completedJob.getStatus().getError() != null) {
            System.out.println(
                "BigQuery was unable to extract due to an error: \n" + job.getStatus().getError());
            return;
          }
          System.out.println(
              "Table export successful. Check in GCS bucket for the " + dataFormat + " file.");
        } catch (BigQueryException | InterruptedException e) {
          System.out.println("Table extraction job was interrupted. \n" + e.toString());
        }
      }

      // Exports datasetName:tableName to destinationUri as raw CSV
      public static void extractTableToCsv(
          String projectId,
          String datasetName,
          String tableName,
          String destinationUri,
          String dataFormat) {
        try {
          // Initialize client that will be used to send requests. This client only needs to be created
          // once, and can be reused for multiple requests.
          BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    
          TableId tableId = TableId.of(projectId, datasetName, tableName);
          Table table = bigquery.getTable(tableId);
    
          Job job = table.extract(dataFormat, destinationUri);
    
          // Blocks until this job completes its execution, either failing or succeeding.
          Job completedJob =
              job.waitFor(
                  RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
                  RetryOption.totalTimeout(Duration.ofMinutes(3)));
          if (completedJob == null) {
            System.out.println("Job not executed since it no longer exists.");
            return;
          } else if (completedJob.getStatus().getError() != null) {
            System.out.println(
                "BigQuery was unable to extract due to an error: \n" + job.getStatus().getError());
            return;
          }
          System.out.println(
              "Table export successful. Check in GCS bucket for the " + dataFormat + " file.");
        } catch (BigQueryException | InterruptedException e) {
          System.out.println("Table extraction job was interrupted. \n" + e.toString());
        }
      }

}