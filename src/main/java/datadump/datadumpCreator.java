package datadump;

import support.Constants;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

public class datadumpCreator {
    public static void createDataDump(String username, String databaseName) throws IOException {
        String DbPath = "./DatabaseSystem/Database/";
        String dataDictionaryPath = DbPath + "/" + databaseName + "/" + Constants.dataDictionaryFileName;
        String sqlDumpPath = DbPath + "/" + databaseName + "/" + Constants.sqlDumpFile;

        File sql_dump_file = new File(sqlDumpPath);
        if(sql_dump_file.exists() && !sql_dump_file.isDirectory()) {
            // delete the sql dump file if exists already
            sql_dump_file.delete();
        }
        else
        {
            // create an empty erd file
            sql_dump_file.createNewFile();
        }

        File data_dictionary_file = new File(dataDictionaryPath);
        if(data_dictionary_file.exists() && !data_dictionary_file.isDirectory() && data_dictionary_file.length() > 0) {
            // data dictionary file exists for sql dump creation
            Scanner fileReader_DataDictionary = new Scanner(data_dictionary_file);
            FileWriter fileWriter_SQLDumpFile = new FileWriter(sql_dump_file);
            String lineInDataDictionary;
            boolean firstLine = true;

            // Database queries
            fileWriter_SQLDumpFile.append("CREATE DATABASE "+ databaseName + ";").append("\n");
            fileWriter_SQLDumpFile.append("USE " + databaseName + ";").append("\n");


            // tables create queries
            while(fileReader_DataDictionary.hasNext())
            {
                String columnsString = "";
                String createStmt = "";

                if(firstLine)
                {
                    // not needed first line of data dictionary in sql dump
                    fileReader_DataDictionary.nextLine();
                    firstLine = false;
                }
                else
                {
                    lineInDataDictionary = fileReader_DataDictionary.nextLine();
                    lineInDataDictionary = lineInDataDictionary.replace(";", "");
                    List<String> tableEntity = List.of(lineInDataDictionary.split(Constants.tableColumnSeparator));
                    String tableName = tableEntity.get(0);
                    List<String> tableColumns = new ArrayList<>(List.of(tableEntity.get(1).split(Constants.columnColumnSeparator)));
                    if(tableColumns.get(tableColumns.size()-1).equals(" ") || tableColumns.get(tableColumns.size()-1).equals(""))
                    {
                        tableColumns.remove(tableColumns.size()-1);
                    }


                    if(lineInDataDictionary.contains(Constants.foreignKey))
                    {
                        String foreignKeyText = tableColumns.get(tableColumns.size()-1);
                        String[] processingArray = foreignKeyText.split(",");
                        String referencedTableName = processingArray[0].split("[(]")[1].trim();
                        String referencedColumnName = processingArray[1].split("[)]")[0].trim();

                        String primarykey = tableColumns.get(tableColumns.size()-2).trim();
                        int startIdx = primarykey.indexOf("(");
                        int endIdx = primarykey.indexOf(")");
                        primarykey = primarykey.substring(startIdx+1,endIdx);
                        String foreignKeyStmt = " FOREIGN KEY REFERENCES "  + referencedTableName + "(" +referencedColumnName + ")";
                        for(int i =0; i< tableColumns.size()-2; i++)
                        {

                            columnsString += tableColumns.get(i);
                            if(tableColumns.get(i).contains(primarykey))
                            {
                                columnsString += " primary key";
                            }
                            if(tableColumns.get(i).contains(referencedColumnName))
                            {
                                columnsString += foreignKeyStmt;
                            }
                            if(i != tableColumns.size()-3)
                            {
                                columnsString += ",";
                            }
                        }

                        createStmt = formCreateTableStatement(tableName, columnsString);

                        fileWriter_SQLDumpFile.write(createStmt + ";");
                        fileWriter_SQLDumpFile.append("\n");
                    }
                    else
                    {
                        String primarykey = tableColumns.get(tableColumns.size()-1).trim();
                        int startIdx = primarykey.indexOf("(");
                        int endIdx = primarykey.indexOf(")");
                        primarykey = primarykey.substring(startIdx+1,endIdx);

                        for(int i =0; i< tableColumns.size()-1; i++)
                        {
                            columnsString += tableColumns.get(i);

                            if(tableColumns.get(i).contains(primarykey))
                            {
                                columnsString += " primary key";
                            }
                            if(i != tableColumns.size()-2)
                            {
                                columnsString += ",";
                            }
                        }

                        createStmt = formCreateTableStatement(tableName, columnsString);
                        fileWriter_SQLDumpFile.write(createStmt + ";");
                        fileWriter_SQLDumpFile.append("\n");
                    }

                    // tables insert queries
                    String tablePath = DbPath + "/"  + databaseName + "/" + tableName.trim() + "/data.txt";
                    List<String> insertDataStmt = formInsertTableStatement(tableName.trim(), tablePath);
                    if(insertDataStmt != null)
                    {
                        for(String insertStmt : insertDataStmt)
                        {
                            fileWriter_SQLDumpFile.write(insertStmt);
                            fileWriter_SQLDumpFile.append("\n");
                        }
                    }
                }

            }

            fileReader_DataDictionary.close();
            fileWriter_SQLDumpFile.close();
        }
    }

    public static String formCreateTableStatement(String tableName, String columns){
        String statement = String.format("CREATE TABLE %s " +
                "(" + "%s " + ")", tableName, columns);

        return  statement;

    }

    public static List<String> formInsertTableStatement(String tableName, String path) throws FileNotFoundException {

        File table_file = new File(path);
        Scanner fileReader_TableData = new Scanner(table_file);
        List<List<String>> dataList = new ArrayList<>();
        while(fileReader_TableData.hasNext())
        {
                List<String> tableData = new ArrayList<>(List.of(fileReader_TableData.nextLine().split(Constants.tableColumnSeparator)));
                if(tableData.get(tableData.size()-1).equals(" ") || tableData.get(tableData.size()-1).equals(""))
                    tableData.remove(tableData.size()-1);
                dataList.add(tableData);
        }

        fileReader_TableData.close();

        if(dataList.size() == 0)
            return null;

        List<String> createStmt = prepareStatements(tableName, dataList);

        return createStmt;
    }

    private static List<String> prepareStatements(String tableName, List<List<String>> rows) {
        List<String> result = new ArrayList<>();
        rows.forEach(row->{
            StringBuilder sb = new StringBuilder();
            for(int i=0;i<row.size();i++){
                sb.append(row.get(i).trim());
                if(i<row.size()-1){
                    sb.append(",");
                }
            }
            String sql = String.format("INSERT INTO %s VALUES (%s);", tableName, sb.toString());
            result.add(sql);
        });
        return result;
    }
}
