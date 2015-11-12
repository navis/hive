// Decompiled by Jad v1.5.8g. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://www.kpdus.com/jad.html
// Decompiler options: packimports(3) 
// Source File Name:   Test.java

package org.apache.hive.jdbc;

import java.io.*;
import java.sql.*;
import java.util.Properties;

public class Test {

  public Test() {
  }

  public static void main(String args[])
      throws Exception {
    String url = args.length != 0 ? args[0] : "jdbc:hive2://localhost:10000";
    if (!url.contains(":")) {
      url = "localhost:" + url;
    }
    if (!url.startsWith("jdbc:hive2://")) {
      url = "jdbc:hive2://" + url;
    }
    Connection connection = new HiveConnection(url, new Properties());
    StringBuilder builder = new StringBuilder();
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
      do {
        if (builder.length() == 0) {
          System.out.print("> ");
        }
        String line;
        if ((line = reader.readLine()) == null)
          break;
        String trimmed = line.trim();
        if (!trimmed.isEmpty())
          if (trimmed.endsWith(";")) {
            builder.append(line.subSequence(0, line.length() - 1));
            execute(connection, builder.toString().trim());
            builder.setLength(0);
          } else {
            builder.append(line).append('\n');
          }
      } while (true);
    } finally {
      connection.close();
    }
  }

  private static void execute(Connection connection, String sql)
      throws Exception {
    String[] params = sql.split("\\s+");
    switch (params[0]) {
      case "$1": {
        String schema = null;
        String tables = null;
        String[] types = null;
        for (int i = 1; i < params.length; i++) {
          switch (params[i].trim()) {
            case "-d":
            case "-s":
              schema = params[++i].trim();
              break;
            case "-t":
              tables = params[++i].trim();
              break;
            case "-x":
              types = params[++i].split(",");
              break;
          }
        }
        printResult(connection.getMetaData().getTables(null, schema, tables, types));
        return;
      }
      case "$2":
        printResult(connection.getMetaData().getSchemas());
        return;
      case "$3":
        printResult(connection.getMetaData().getTableTypes());
        return;
      case "$4": {
        String schema = null;
        String tables = null;
        String columns = null;
        for (int i = 1; i < params.length; i++) {
          switch (params[i].trim()) {
            case "-d":
            case "-s":
              schema = params[++i].trim();
              break;
            case "-t":
              tables = params[++i].trim();
              break;
            case "-c":
              columns = params[++i].trim();
              break;
          }
        }
        printResult(connection.getMetaData().getColumns(null, schema, tables, columns));
        return;
      }
    }
    Statement statement = connection.createStatement();
    try {
      if (statement.execute(sql)) {
        printResult(statement.getResultSet());
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      statement.close();
    }
  }

  private static void printResult(ResultSet result)
      throws SQLException {
    String prefix = "SQL-" + counter++ + " --> ";
    try {
      int columns = result.getMetaData().getColumnCount();
      StringBuilder x = new StringBuilder();
      for (; result.next(); ) {
        for (int i = 1; i <= columns; i++) {
          if (x.length() > 0) {x.append(", ");}
          x.append(String.valueOf(result.getObject(i)));
        }
        System.out.println(prefix + x);
        x.setLength(0);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      result.close();
    }
  }

  private static int counter;
}
