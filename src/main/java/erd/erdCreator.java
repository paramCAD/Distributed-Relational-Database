package erd;

import support.Constants;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Scanner;

public class erdCreator {

    public static void createERDDiagram(String username, String databaseName) throws IOException {
        String DbPath = "./DatabaseSystem/Database/";
        String dataDictionaryPath = DbPath + "/" + databaseName + "/" + Constants.dataDictionaryFileName;
        String erdDiagramPath = DbPath + "/" + databaseName + "/" + Constants.erdDiagramFileName;

        File erd_diagram_file = new File(erdDiagramPath);
        if(erd_diagram_file.exists() && !erd_diagram_file.isDirectory()) {
            // delete the erd file if exists already
            erd_diagram_file.delete();
        }
        else
        {
            // create an empty erd file
            erd_diagram_file.createNewFile();
        }

        File data_dictionary_file = new File(dataDictionaryPath);
        if(data_dictionary_file.exists() && !data_dictionary_file.isDirectory() && data_dictionary_file.length() > 0) {
            // data dictionary file exists for erd creation
            Scanner fileReader_DataDictionary = new Scanner(data_dictionary_file);
            FileWriter fileWriter_ERDDiagram = new FileWriter(erd_diagram_file);
            String lineInDataDictionary;
            boolean firstLine = true;

            while(fileReader_DataDictionary.hasNext())
            {
                if(firstLine)
                {
                    // not needed first line of data dictionary in erd diagram
                    firstLine = false;
                }
                else
                {
                    lineInDataDictionary = fileReader_DataDictionary.nextLine();
                    if(lineInDataDictionary.contains(Constants.foreignKey))
                    {
                        handleTableWithForeignKeyRelation(lineInDataDictionary, fileWriter_ERDDiagram);
                    }
                    else
                    {
                        handleNormalTable(lineInDataDictionary, fileWriter_ERDDiagram);
                    }
                }
            }

            fileReader_DataDictionary.close();
            fileWriter_ERDDiagram.close();
        }
    }

    public  static void handleNormalTable(String lineInDataDictionary, FileWriter fileWriter_ERDDiagram) throws IOException {
        fileWriter_ERDDiagram.write(lineInDataDictionary);
        fileWriter_ERDDiagram.append("\n");
    }

    public static void handleTableWithForeignKeyRelation(String lineInDataDictionary, FileWriter fileWriter_ERDDiagram) throws IOException {
        List<String> tableEntity = List.of(lineInDataDictionary.split(Constants.tableColumnSeparator));
        String tableName = tableEntity.get(0);

        List<String> tableColumns = List.of(tableEntity.get(1).split(Constants.columnColumnSeparator));

        String foreignKeyText = tableColumns.get(tableColumns.size()-1); // FOREIGN_KEY (MainTableName,ColumnName);

        String[] processingArray = foreignKeyText.split(",");
        String referencedTableName = processingArray[0].split("[(]")[1];
        fileWriter_ERDDiagram.write(lineInDataDictionary);
        fileWriter_ERDDiagram.append("\n");
        fileWriter_ERDDiagram.write(tableName + Constants.foreignKeySeparator +referencedTableName);
        fileWriter_ERDDiagram.append("\n");
    }
}
