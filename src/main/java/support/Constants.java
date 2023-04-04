package support;

public class Constants{
    public static final String createDatabaseRegex  = "(CREATE|create)\\s+(DATABASE|database)\\s+\\w+\\s*(;)";
    public static final String createTableRegex = "(CREATE|create)\\s+(TABLE|table)\\s*\\w+\\((.*?)\\)(;)";
    public static final String useDatabaseRegex = "(use|USE)\\s+\\w+\\s*(;)";
    public static final String outputFolderPath = "./output/";
    public static final String foreignKey = "FOREIGN_KEY";
    public static final String primarykey = "PRIMARY_KEY";
    public static final String dataDictionaryFileName = "data_dictionary.txt";
    public static final String erdDiagramFileName = "erd_diagram.txt";
    public static final String tableColumnSeparator = "<xx>";
    public static final String columnColumnSeparator = "<x>";
    public static final String sqlDumpFile = "sql_dump.sql";
    public static final String foreignKeySeparator = " ==(*)=========(1)==>> ";

}
