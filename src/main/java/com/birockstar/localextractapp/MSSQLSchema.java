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
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.google.gson.Gson;
import java.lang.reflect.Type;
import com.google.gson.reflect.TypeToken;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;

public class MSSQLSchema {

    private static final String CONNECTIONURL = "jdbc:sqlserver://localhost:1433;databaseName=YOUR_DATABASE_NAME;user=YOUR_USER_NAME;password=YOUR_PASSWORD";

    public static void main(String[] args) {

        /*
         * String sample =
         * "{\"name\":\"ID\",\"type\":\"INTEGER\"},{\"name\":\"STOCKNO\",\"type\":\"STRING\"},{";
         * 
         * System.out.println(sample.substring(30,35));
         */

        // gsonTesting();

        //System.out.println(gettingStarted());

        Map<String, String> m = getSQLSchema("STOCK");

        for (Map.Entry<String, String> e : m.entrySet()) {

            System.out.println(e.getKey() + " " + e.getValue());

        }

    }

    public static Map<String, String> getSQLSchema(String tableName) {

        //System.out.println("getting schema query");

        Map<String, String> m = new LinkedHashMap<>();

        String query = "select ORDINAL_POSITION, COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '" + tableName
                + "' order by 1 asc";

        try (Connection connection = DriverManager.getConnection(CONNECTIONURL)) {

            try (PreparedStatement statement = connection.prepareStatement(query)) {

                ResultSet rs = statement.executeQuery();

                while (rs.next()) {

                    //System.out.println(rs.getString(2) + rs.getString(3));
  
                    String col = rs.getString(2);
                    String type = rs.getString(3);

                    //System.out.println("Adding: " + col + " - " + type);

                    m.put(col, type);
                }

            }

        }

        catch (SQLException e) {

            e.printStackTrace();
        }

        return m;

    }

    public static Map<String, String> gsonTesting() {

        Map<String, String> m = new HashMap<>();

        Gson gson = new Gson();

        Type listType = new TypeToken<List<FieldNameType>>() {
        }.getType();

        List<FieldNameType> fnt = gson.fromJson(gettingStarted(), listType);

        for (FieldNameType f : fnt) {

            m.put(f.getName(), f.getType());

            // System.out.println(f.getName() + "-" + f.getType());

        }

        return m;

    }

    public static void toDynamString() {

        String mssqlschema = "[ID] [int] IDENTITY(1,1) NOT NULL, 			\n"
                + "[STOCKNO] [nvarchar](50) NULL,              \n" + "[fkStockLocationID] [int] NULL,             \n"
                + "[OPENING] [float] NULL,                     \n" + "[OPENINGVAL] [float] NULL,                  \n"
                + "[OpeningUnitCost] [money] NULL,             \n" + "[CLOSING] [float] NULL,                     \n"
                + "[CLOSEVAL] [float] NULL,                    \n" + "[UnitCost] [money] NULL,                    \n"
                + "[THSTOCKUSE] [float] NULL,                  \n" + "[THSTUSER] [float] NULL,                    \n"
                + "[ACSTOCKUSE] [float] NULL,                  \n" + "[ACSTUSER] [float] NULL,                    \n"
                + "[THSTOCKPROC] [float] NULL,                 \n" + "[THSTOCKPROCR] [money] NULL,                \n"
                + "[STOCKMANUF] [float] NULL,                  \n" + "[STOCKMANUFR] [money] NULL,                 \n"
                + "[STOCKPCH] [float] NULL,                    \n" + "[StockPchValue] [float] NULL,               \n"
                + "[XferQtyOut] [float] NULL,                  \n" + "[XferQtyIn] [float] NULL,                   \n"
                + "[XferNetQty] [float] NULL,                  \n" + "[XferValueOut] [float] NULL,                \n"
                + "[XferValueIn] [float] NULL,                 \n" + "[XferNetValue] [float] NULL,                \n"
                + "[WastedQty] [float] NULL,                   \n" + "[WastedValue] [float] NULL,                 \n"
                + "[VARIANCE] [float] NULL,                    \n" + "[VARPCT] [float] NULL,                      \n"
                + "[VARRANDS] [float] NULL,                    \n" + "[CalcOpen] [bit] NULL,                      \n"
                + "[CalcClose] [bit] NULL,                     \n" + "[StockTakeDate] [datetime] NULL,            \n"
                + "[IsSold] [bit] NULL                         ";

        String[] lines = mssqlschema.split("\n");

        List<String> lns = Arrays.asList(lines);

        lns.stream()
                .forEach(a -> System.out.println(a.replace("NOT NULL,", "").replace("NULL,", "").replace("NULL", "")
                        .replace("IDENTITY(1,1)", "").replace("[money]", "\", StandardSQLTypeName.FLOAT64),")
                        .replace("[nvarchar](50)", "\", StandardSQLTypeName.STRING),")
                        .replace("[int]", "\", StandardSQLTypeName.INT64),")
                        .replace("[float]", "\", StandardSQLTypeName.FLOAT64),")
                        .replace("[datetime]", "\", StandardSQLTypeName.DATETIME),")
                        .replace("[bit]", "\", StandardSQLTypeName.INT64),").replace("[", "Field.of(\"")
                        .replace("] ", "").trim()));

    }

    public static void toJAVA() {

        String mssqlschema = "[ID] [int] IDENTITY(1,1) NOT NULL, 			\n"
                + "[STOCKNO] [nvarchar](50) NULL,              \n" + "[fkStockLocationID] [int] NULL,             \n"
                + "[OPENING] [float] NULL,                     \n" + "[OPENINGVAL] [float] NULL,                  \n"
                + "[OpeningUnitCost] [money] NULL,             \n" + "[CLOSING] [float] NULL,                     \n"
                + "[CLOSEVAL] [float] NULL,                    \n" + "[UnitCost] [money] NULL,                    \n"
                + "[THSTOCKUSE] [float] NULL,                  \n" + "[THSTUSER] [float] NULL,                    \n"
                + "[ACSTOCKUSE] [float] NULL,                  \n" + "[ACSTUSER] [float] NULL,                    \n"
                + "[THSTOCKPROC] [float] NULL,                 \n" + "[THSTOCKPROCR] [money] NULL,                \n"
                + "[STOCKMANUF] [float] NULL,                  \n" + "[STOCKMANUFR] [money] NULL,                 \n"
                + "[STOCKPCH] [float] NULL,                    \n" + "[StockPchValue] [float] NULL,               \n"
                + "[XferQtyOut] [float] NULL,                  \n" + "[XferQtyIn] [float] NULL,                   \n"
                + "[XferNetQty] [float] NULL,                  \n" + "[XferValueOut] [float] NULL,                \n"
                + "[XferValueIn] [float] NULL,                 \n" + "[XferNetValue] [float] NULL,                \n"
                + "[WastedQty] [float] NULL,                   \n" + "[WastedValue] [float] NULL,                 \n"
                + "[VARIANCE] [float] NULL,                    \n" + "[VARPCT] [float] NULL,                      \n"
                + "[VARRANDS] [float] NULL,                    \n" + "[CalcOpen] [bit] NULL,                      \n"
                + "[CalcClose] [bit] NULL,                     \n" + "[StockTakeDate] [datetime] NULL,            \n"
                + "[IsSold] [bit] NULL                         ";

        String[] lines = mssqlschema.split("\n");

        List<String> lns = Arrays.asList(lines);

        lns.stream()
                .forEach(a -> System.out.println(a.replace("NOT NULL,", "").replace("NULL,", "").replace("NULL", "")
                        .replace("IDENTITY(1,1)", "").replace("[money]", "\", StandardSQLTypeName.FLOAT64),")
                        .replace("[nvarchar](50)", "\", StandardSQLTypeName.STRING),")
                        .replace("[int]", "\", StandardSQLTypeName.INT64),")
                        .replace("[float]", "\", StandardSQLTypeName.FLOAT64),")
                        .replace("[datetime]", "\", StandardSQLTypeName.DATETIME),")
                        .replace("[bit]", "\", StandardSQLTypeName.INT64),").replace("[", "Field.of(\"")
                        .replace("] ", "").trim()));

    }

    public static String gettingStarted() {

        String mssqlschema = "[ID] [int] IDENTITY(1,1) NOT NULL, 			\n"
                + "[STOCKNO] [nvarchar](50) NULL,              \n" + "[fkStockLocationID] [int] NULL,             \n"
                + "[OPENING] [float] NULL,                     \n" + "[OPENINGVAL] [float] NULL,                  \n"
                + "[OpeningUnitCost] [money] NULL,             \n" + "[CLOSING] [float] NULL,                     \n"
                + "[CLOSEVAL] [float] NULL,                    \n" + "[UnitCost] [money] NULL,                    \n"
                + "[THSTOCKUSE] [float] NULL,                  \n" + "[THSTUSER] [float] NULL,                    \n"
                + "[ACSTOCKUSE] [float] NULL,                  \n" + "[ACSTUSER] [float] NULL,                    \n"
                + "[THSTOCKPROC] [float] NULL,                 \n" + "[THSTOCKPROCR] [money] NULL,                \n"
                + "[STOCKMANUF] [float] NULL,                  \n" + "[STOCKMANUFR] [money] NULL,                 \n"
                + "[STOCKPCH] [float] NULL,                    \n" + "[StockPchValue] [float] NULL,               \n"
                + "[XferQtyOut] [float] NULL,                  \n" + "[XferQtyIn] [float] NULL,                   \n"
                + "[XferNetQty] [float] NULL,                  \n" + "[XferValueOut] [float] NULL,                \n"
                + "[XferValueIn] [float] NULL,                 \n" + "[XferNetValue] [float] NULL,                \n"
                + "[WastedQty] [float] NULL,                   \n" + "[WastedValue] [float] NULL,                 \n"
                + "[VARIANCE] [float] NULL,                    \n" + "[VARPCT] [float] NULL,                      \n"
                + "[VARRANDS] [float] NULL,                    \n" + "[CalcOpen] [bit] NULL,                      \n"
                + "[CalcClose] [bit] NULL,                     \n" + "[StockTakeDate] [datetime] NULL,            \n"
                + "[IsSold] [bit] NULL                         ";

        String[] lines = mssqlschema.split("\n");

        StringBuilder sb = new StringBuilder();

        List<String> lns = Arrays.asList(lines);

        lns.stream().forEach(a -> sb.append(a.replace("NOT NULL,", "").replace("NULL,", "").replace("NULL", "")
                .replace("IDENTITY(1,1)", "").replace("[money]", "\",\"type\":\"FLOAT64")
                .replace("[nvarchar](50)", "\",\"type\":\"STRING").replace("[int]", "\",\"type\":\"INT64")
                .replace("[float]", "\",\"type\":\"FLOAT64").replace("[datetime]", "\",\"type\":\"DATETIME")
                .replace("[bit]", "\",\"type\":\"INT64").replace("[", "\"},{\"name\":\"").replace("] ", "").trim()));

        String rearrange = sb.substring(0, 2);

        sb.replace(0, 3, "[");
        sb.append(rearrange + "]");

        return sb.toString();

        // System.out.println(sb.toString());

        /*
         * lns.stream().forEach(a->System.out.println(a.replace("NOT NULL,",
         * "").replace("NULL,", "").replace("NULL", "").replace("IDENTITY(1,1)", "")
         * .replace("[nvarchar](50)", "\",\"type\":\"STRING").replace("[int]",
         * "\",\"type\":\"INTEGER").replace("[float]", "\",\"type\":\"FLOAT")
         * .replace("[datetime]", "\",\"type\":\"STRING").replace("[bit]",
         * "\",\"type\":\"INTEGER").replace("[", "\"},{\"name\":\"").replace("] ",
         * "")));
         */

        /*
         * mssqlschema = mssqlschema.replace("NOT NULL,", "").replace("NULL,",
         * "").replace("IDENTITY(1,1)", "") .replace("[nvarchar](50)",
         * "\",\"type\":\"STRING").replace("[int]",
         * "\",\"type\":\"INTEGER").replace("[float]", "\",\"type\":\"FLOAT")
         * .replace("[datetime]", "\",\"type\":\"STRING").replace("[bit]",
         * "\",\"type\":\"INTEGER").replace("[", "\"},{\"name\":\"").replace("] ", "");
         * 
         * System.out.println(mssqlschema);
         */

    }
}
