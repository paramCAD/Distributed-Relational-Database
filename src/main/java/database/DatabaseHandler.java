package database;

import logmanagement.EventLogs;
import org.apache.commons.io.FileUtils;
import support.Constants;
import support.GlobalData;

import java.io.*;
import java.lang.invoke.VarHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.regex.*;


public class DatabaseHandler {

    public static final String seprator = " <xx> ";
    public static  final String ColumnSeprator = " <x> ";
    public static final String space = " ";
    public static  String User1DB ="null";

    static EventLogs eventLogs = new EventLogs();

    public static String DbPath = "./DatabaseSystem/Database/";

    public static boolean CheckSpecial(String chk) {
            Pattern pattern = Pattern.compile("[^a-zA-Z0-9_]");
            Matcher matcher = pattern.matcher(chk);
            return matcher.find();
    }

    public static boolean isNumeric(String string) {
        int intValue;
        if(string == null || string.equals("")) {
            System.out.println("String cannot be parsed, it is null or empty.");
            return false;
        }

        try {
            intValue = Integer.parseInt(string);
            return true;
        } catch (NumberFormatException e) {
            System.out.println("Input String cannot be parsed to Integer.");
        }
        return false;
    }

    public static boolean checkForeign(String ref,String DbName) throws IOException {

            String  [] validator = ref.replaceAll("[\\)\\(]", " ").trim().split(" ");
            String tableName = validator[0].trim();
            String ColumName = validator[1].trim();
            String path = DbPath + DbName + "/" + tableName;
            File TableDir = new File(path);
            if (TableDir.exists()) {
                path +=  "/meta.txt";
                File f = new File(path);
                BufferedReader bf = new BufferedReader(new FileReader(f));
                String line;
                while ((line = bf.readLine()) != null) {
                    String[] words = line.split(Pattern.quote(seprator));
                    for (int i = 0; i < words.length; i++) {
                        String []tmp = words[i].split(" ");
                        if(tmp[0].trim().equals(ColumName) && tmp.length > 1 && tmp[1].trim().equals("primarykey")){
                            return true;
                        }
                    }
                }
            }
            else{
                Instant timestamp = Instant.now();
                eventLogs.writeEventLogs
                        (" Error: The referenced table does not exist", timestamp, "", DbName, "");
                System.out.println("The referenced table does not exist");
                return false;
            }
        Instant timestamp = Instant.now();
        eventLogs.writeEventLogs
                (" Error: The referenced column does not exist", timestamp, "", DbName, "");
            System.out.println("Referenced Column does not exist");
            return false;
        }

    public static String addSeprator(String res,String ColumnName,String seprator){
        res += ColumnName;
        res += space;
        res += seprator;
        res += space;
        return res;
    }

    public static boolean checkDbExist(String DbName){
        File f = new File(DbPath + DbName);
        if(!f.isDirectory()){

            System.out.println("DataBase Does not Exist!");
            return false;
        }
        return true;
    }

    public static boolean useDb(String query) throws IOException {
        query= query.trim().replaceAll(" +"," ");
        String [] words = query.split(" ");
        if(!words[0].equals("use") || words.length != 2){
            return false;
        }

        String dataBaseName = words[1].trim();
        if(!checkDbExist(dataBaseName)){
            return false;
        }
        User1DB = dataBaseName;
        Instant timestamp = Instant.now();

        String DbName = null;
        eventLogs.writeEventLogs
                (" Query executed successfully", timestamp, query, DbName, "Use database");
        return true;
    }

    public static boolean CreateTable(String query,String DbName) throws IOException {
        query= query.trim().replaceAll(" +"," ");

        if(!checkDbExist(DbName)){

            Instant timestamp = Instant.now();
            eventLogs.writeEventLogs
                    ("Error: Select the Database first to create a table", timestamp, query, DbName, "Create Table");
            return false;
        }

        String DataDictIns = "";
        String primaryKey = "";
        String foreingKey = "";
        HashMap<String, Integer> DataType = new HashMap<>() {
            {
                put("int", 1);
                put("varchar(255)", 1);
            }
        };
        HashMap<String, Integer> SQLkeys = new HashMap<>() {
            {
                put("primary", 1);
                put("foreign", 1);
            }
        };
        if (DbName == "None") {
            System.out.println("You should first select the Database to create a table");
            Instant timestamp = Instant.now();
            eventLogs.writeEventLogs
                    ("Error: Select the Database first to create a table", timestamp, query, DbName, "Create Table");
            return false;
        }
        String path = DbPath + DbName + "/";
        query = query.toLowerCase();
        String arr[] = query.split(" ");
//          Checking for the create keyword
        if (!arr[0].equals("create")) {
            System.out.println("Please Enter Valid Query: Error in " + arr[0] + "  KEYWORD");
            return false;
        }
//           Checking for the table keyword
        if (!arr[1].equals("table")) {
            System.out.println("Please Enter Valid Query: Error in " + arr[1] + "KEYWORD");
            return false;
        }
//      Checking for the tableName

        String TableName = "";
        String[] TableSeprate = arr[2].split("\\(");
        TableName = TableSeprate[0];
        if (CheckSpecial(TableName)) {
            System.out.println(arr[2] + " contains special character");
        }
        DataDictIns = addSeprator(DataDictIns,TableName, seprator);
        int lastC = 0;
        for (int i = 0; i < query.length(); i++) {
            if (query.charAt(i) == '(') {
                lastC = i;
                break;
            }
        }
//      Checking for the validation of end of the query
        String Columns = query.substring(lastC + 1, query.length() - 1);
        if (!query.substring(query.length() - 1, query.length()).equals(")")) {
            Instant timestamp = Instant.now();
            eventLogs.writeEventLogs
                    ("Error: Invalid query entered", timestamp, query, DbName, "Create Table");
            System.out.println("Query is not valid");
        }
//          Checking for the columns of the table
        String ColInsert = "";
        String DataInsert  = "";

        HashMap<String,Integer> Coldup = new HashMap<>();
        String[] column = Columns.split(",");
        for (String col : column) {
            col = col.trim();
            String[] curCol = col.split(" ");
            if (curCol.length < 2) {
                Instant timestamp = Instant.now();
                eventLogs.writeEventLogs
                        ("Error: Invalid query entered", timestamp, query, DbName, "Create Table");
                System.out.println("Invalid Query");
                return false;
            }
            for(int j = 0 ; j < curCol.length; j ++){
//                  Checking for the name of the column
                if(j == 0 && !SQLkeys.containsKey(curCol[j]) && !DataType.containsKey(curCol[j]) && !(CheckSpecial(curCol[j])) && !Coldup.containsKey(curCol[j])){
                    ColInsert += curCol[j];
                    DataDictIns += curCol[j] + " ";
                    Coldup.put(curCol[j],1);
                }
//                  Checking for the  datatype of the column
                else if(j == 1 && DataType.containsKey(curCol[j])){
                    DataDictIns += curCol[j] + " ";
                    DataInsert += curCol[j];
                }
                else if(j == 1 && !DataType.containsKey(curCol[j])){
                    Instant timestamp = Instant.now();
                    eventLogs.writeEventLogs
                            ("Error: Incorrect datatype entered by the user", timestamp, query, DbName, "Create Table");
                    System.out.println("Wrong Datatype entered");
                    return false;
                }
//                   checking for the sql keys of the table
                if(j >= 2){
                    if(SQLkeys.containsKey(curCol[j].trim())){
                        if(curCol[j].trim().equals("primary")){
                            if(j + 1 < curCol.length && curCol[j + 1].equals("key")) {
                                ColInsert += " primarykey";
                                if(!primaryKey.equals("")){
                                    Instant timestamp = Instant.now();
                                    eventLogs.writeEventLogs
                                            ("Sorry! more then 1 primary key are currently out of scope!", timestamp, query, DbName, "Create Table");
                                    System.out.println("Sorry! more then 1 primary key are currently out of scope!");
                                    return false;
                                }
                                else{
                                    primaryKey += "PRIMARY_KEY(" + curCol[0] + ") ";
                                }
                                j++;
                            }
                            else{
                                Instant timestamp = Instant.now();
                                eventLogs.writeEventLogs
                                        ("Error: Incorrect primary key keyword", timestamp, query, DbName, "Create Table");
                                System.out.println("Problem in primary key keyword");
                                return false;
                            }
                        }
                        else if(curCol[j].equals("foreign")) {
                            if (curCol.length - j >= 4) {
                                if (curCol[j + 1].equals("key") &&
                                        curCol[j + 2].equals("references")) {
                                    if (checkForeign(curCol[j + 3],DbName)) {

                                        String []TableColumn = curCol[j + 3].replaceAll("[\\)\\(]", " ").trim().split(" ");
                                        if(!foreingKey.equals("")){
                                            Instant timestamp = Instant.now();
                                            eventLogs.writeEventLogs
                                                    ("Sorry! Two foreign keys are currently out of scope!", timestamp, query, DbName, "Create Table");

                                            System.out.println("Sorry! Two foreign keys are currently out of scope!");
                                            return false;
                                        }
                                        foreingKey += "FOREIGN_KEY (" + TableColumn[0] + " , "+ TableColumn[1] + " )";
                                        ColInsert += " foreignkey";

                                    } else {
                                        Instant timestamp = Instant.now();
                                        eventLogs.writeEventLogs
                                                ("Error: Incorrect foreign key", timestamp, query, DbName, "Create Table");
                                        System.out.println("Problem in foreign key");
                                        return false;
                                    }
                                }
                                else{
                                    Instant timestamp = Instant.now();
                                    eventLogs.writeEventLogs
                                            ("Error: Incorrect keyword references or foreign key", timestamp, query, DbName, "Create Table");
                                    System.out.println("Incorrect keyword reference or foreign key");
                                    return false;
                                }
                                j+= 4;
                            }
                            else{
                                Instant timestamp = Instant.now();
                                eventLogs.writeEventLogs
                                        ("Error: Incorrect foreign key", timestamp, query, DbName, "Create Table");
                                System.out.println("foreign key not valid");
                                return false;
                            }
                        }
                        else{
                            Instant timestamp = Instant.now();
                            eventLogs.writeEventLogs
                                    ("Error: Incorrect key attribute", timestamp, query, DbName, "Create Table");
                            System.out.println("Not a valid key attribute");
                            return false;
                        }
                    }
                    else{
                        Instant timestamp = Instant.now();
                        eventLogs.writeEventLogs
                                ("Error: Incorrect primary key keyword", timestamp, query, DbName, "Create Table");
                        System.out.println("Not a valid key");
                        return false;
                    }
                }
            }
            ColInsert += seprator;
            DataInsert += seprator;
            DataDictIns += ColumnSeprator;
        }
        path = path + TableName;

        File TableDir = new File(path);
        if(TableDir.exists()) {
            Instant timestamp = Instant.now();
            eventLogs.writeEventLogs
                    ("Error: Table already exists", timestamp, query, DbName, "Create Table");
            System.out.println("Table Already Exist!");
            return false;
        }
        TableDir.mkdir();
        String datafile = path + "/data.txt";
        String metafile = path + "/meta.txt";
        File MetaFile  = new File(metafile);
        File DataFile  = new File(datafile);
        FileWriter fw = new FileWriter(MetaFile);
        MetaFile.createNewFile();
        DataFile.createNewFile();

        fw.write(ColInsert+ "\n") ;
        fw.write(DataInsert);
        fw.close();
        String DataDicEntry  = DataDictIns   + primaryKey  + ColumnSeprator + foreingKey + ";";
        createDataDictionary(DataDicEntry, DbName);

        Instant timestamp = Instant.now();
        eventLogs.writeEventLogs
                ("Query executed successfully", timestamp, query, DbName, "Create Table");
        return true;
    }

    public static void createDataDictionary(String data, String DbName) throws IOException {
        File dataDictionaryFile = new File(DbPath + "/" + DbName + "/" + Constants.dataDictionaryFileName);
        if(dataDictionaryFile.exists() ) {
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(dataDictionaryFile, true));
            bufferedWriter.append(data);
            bufferedWriter.append("\n");
            bufferedWriter.close();
        }
        else
        {
            dataDictionaryFile.createNewFile();
            FileWriter fileWriter = new FileWriter(dataDictionaryFile);
            fileWriter.write("TABLES\t <xx> \tCOLUMNS");
            fileWriter.append("\n");
            fileWriter.write(data);
            fileWriter.append("\n");
            fileWriter.close();
        }
    }

    public static void showDatabase(){

        String path = DbPath;
        File f = new File(path);
        String[] DataBase = f.list();
        for (int i = 0; i < DataBase.length; i++) {
                System.out.println(DataBase[i]);
            }
        }

    public static boolean CreateDatabase(String query) throws IOException {
        query= query.trim().replaceAll(" +"," ");

        String origninalQuery = query;
        String[] orginalWords = origninalQuery.split(" ");
        query = query.toLowerCase();
        String[] words = query.split(" ");
        if (!words[0].equals("create") && !words[1].equals("database") || words.length != 3) {
            return false;
        }
        String path = DbPath + orginalWords[2].trim();
        File f = new File(path);
        String DbName = null;
        if (f.exists()) {
            Instant timestamp = Instant.now();
            DbName = null;
            eventLogs.writeEventLogs
                    ("Error: Database already exists", timestamp, query, DbName, "Create Database");
            System.out.println("Database Already Exist");
            return false;
        }
        f.mkdir();
        Instant timestamp = Instant.now();
        eventLogs.writeEventLogs
                ("Query executed successfully", timestamp, query, DbName, "Create Table");
        return true;
    }

    public static boolean CheckTableExist(String DbName,String tableName) throws IOException {

        if(!checkDbExist(DbName)){
            
            return false;
        }

        String Cpath = DbPath + DbName + "/" + tableName;
        File f = new File(Cpath);
            if(f.exists()){
                return true;
            }
            return false;
    }

    public static boolean OperationUD(String DbName ,String tableName, String ColumnName, HashMap<String,Integer> oldVal, HashMap<Integer,String> updateVal , int flag, int NotIn) throws IOException {

        String curPath = DbPath + DbName + "/" + tableName;
        String metaPath = curPath + "/meta.txt";
        String dataPath = curPath + "/data.txt";
        File fm = new File(metaPath);
        BufferedReader bm = new BufferedReader(new FileReader(fm));
        String line = bm.readLine();
        bm.close();
        String Col[] = line.split(Pattern.quote(seprator));
        int IsColumnExist = -1;
        for (int i = 0; i < Col.length; i++) {
            String[] Colchk = Col[i].split(" ");

            if (Colchk.length == 2) {
                for (int j = 0; j < Colchk.length; j++) {
                    if (Colchk[j].equals(ColumnName)) {
                        IsColumnExist = i;
                        break;
                    } else {
                        continue;
                    }
                }
            }
            if (Col[i].equals(ColumnName)) {
                IsColumnExist = i;
                break;
            }
        }
        String query = null;
        if (IsColumnExist == -1 && ColumnName.length() > 0) {
            Instant timestamp = Instant.now();
            query = null;
            String queryType = "";
            if (flag == 0) {
                queryType = "Update Table";
            } else {
                queryType = "Delete Table";
            }
            eventLogs.writeEventLogs
                    ("Error: Column does not exist", timestamp, query, DbName, queryType);
            System.out.println("Column Does not Exist!");
            return false;
        }
        File fr = new File(dataPath);
        BufferedReader br = new BufferedReader(new FileReader(fr));
        File ft = new File(curPath + "/temp.txt");
        FileWriter fw = new FileWriter(ft);
        while ((line = br.readLine()) != null) {
            String words[] = line.split(Pattern.quote(seprator));
            if (flag == 0) {
                for (int i = 0; i < words.length; i++) {
                    if (ColumnName.length() == 0 || oldVal.containsKey(words[IsColumnExist].trim())) {
                        if (NotIn == 0) {
                            if (updateVal.containsKey(i)) {
                                words[i] = updateVal.get(i);
                            }
                        }
                    } else {
                        if (NotIn == 1) {
                            if (updateVal.containsKey(i)) {
                                words[i] = updateVal.get(i);
                            }
                        }
                    }
                }
                String temp = String.join(seprator, words);
                fw.write(temp + "\n");

            } else if (flag == 1) {
                if (IsColumnExist != -1 && !oldVal.containsKey(words[IsColumnExist].trim())) {
                    if (NotIn == 0) {
                        String temp = String.join(seprator, words);
                        fw.write(temp + "\n");
                    }
                } else {
                    if (NotIn == 1) {
                        String temp = String.join(seprator, words);
                        fw.write(temp + "\n");
                    }
                }
            }
        }

        fw.close();
        br.close();
        System.gc();
        if (ft.exists()) {
            FileUtils.delete(fr);
            ft.renameTo(fr);
        }
        Instant timestamp = Instant.now();
        eventLogs.writeEventLogs
                ("Query executed successfully", timestamp, query, DbName, "Update Table");
        return true;
    }

    public static boolean CheckUpdate(String query,String DbName) throws IOException {
        query= query.trim().replaceAll(" +"," ");

        if(!checkDbExist(DbName)){
            return false;
        }
            query = query.toLowerCase();
            String []words = query.split(" ");
            if(words.length < 4){
                return false;
            }
            if(!words[0].equals("update") || !words[2].equals("set")){
                Instant timestamp = Instant.now();
                eventLogs.writeEventLogs
                        ("Error: Problem in the keyword - 'Update' or 'Set' ", timestamp, query, DbName, "Update Table");
                System.out.println("Problem in the keyword update or set");
                return false;
            }

            if(!CheckTableExist(DbName,words[1])){
                Instant timestamp = Instant.now();
                eventLogs.writeEventLogs
                        ("Error: Table already exists", timestamp, query, DbName, "Update Table");
                System.out.println("Table does not exist " + words[1]); 
                return false;
            }

            HashMap<Integer,String > UpdateVal = new HashMap<>();
            HashMap<String,Integer> oldVal = new HashMap<>();
            HashMap<String,Integer> Tcol = new HashMap<>();
            int Notin = 0;

            String []temp = query.split("set");
            String  AfterSet = temp[1];
            String[] temp1 = AfterSet.split("where");
            if(temp1.length < 2){
                Instant timestamp = Instant.now();
                eventLogs.writeEventLogs
                        ("Error: Where condition is empty", timestamp, query, DbName, "Update Table");
                System.out.println("Empty Where condition!");
                return false;
            }

            String col =  temp1[0].trim();
            if(col.length() == 0 || col.isEmpty()){
                Instant timestamp = Instant.now();
                eventLogs.writeEventLogs
                        ("Error: Invalid query entered", timestamp, query, DbName, "Update Table");
                System.out.println(" IN VALID QUERY!");
                return false;
            }


            String curPath = DbPath + DbName +"/" + words[1] + "/meta.txt";
            File f = new File(curPath);
            BufferedReader bf = new BufferedReader(new FileReader(f));
            String tableCol = bf.readLine();
            String [] tmp = tableCol.split(Pattern.quote(seprator));

            for(Integer i  = 0 ; i < tmp.length; i ++){
                if(tmp[i].contains(" ")){
                    String[] tmp1 = tmp[i].split(" ");
                    Tcol.put(tmp1[0],i);
                    continue;
                }
                Tcol.put(tmp[i],i);
            }
            String[] Columns = col.split(",");
            if(Columns.length == 0) { Columns[0] = col.trim(); }
            if(Columns.length % 2 != 0 && Columns.length != 1){
                Instant timestamp = Instant.now();
                eventLogs.writeEventLogs
                        ("Error: Invalid Where condition", timestamp, query, DbName, "Update Table");
                System.out.println("Not valid where condition !");
                return false;
            }
            for(int i = 0; i < Columns.length; i ++){
                String []CurCol = Columns[i].trim().split("=");
                CurCol[0] = CurCol[0].trim();
                CurCol[1] = CurCol[1].trim();
                if(Tcol.containsKey(CurCol[0])){
                    UpdateVal.put(Tcol.get(CurCol[0]) , CurCol[1]);
                }
                else{
                    Instant timestamp = Instant.now();
                    eventLogs.writeEventLogs
                            ("Error: Column does not exist", timestamp, query, DbName, "Update Table");
                    System.out.println("Column " + CurCol[0] + " Does not Exist!" );
                    return false;
                }
            }

            if(!query.contains("where")) {
                return OperationUD(DbName,words[1],"",new HashMap<>(),UpdateVal,0,1);
            }
            else{
                String[] whereCol = new String[0];
                if(temp1[1].contains("=") || temp1[1].contains("!=") || temp1[1].contains("in") || temp1[1].contains("not in")) {
                   if((temp1[1].contains("="))) { whereCol = temp1[1].split("=");}
                    if((temp1[1].contains("!="))) { whereCol = temp1[1].split("!="); Notin = 1;}
                    if((temp1[1].contains("in"))) { whereCol = temp1[1].split("in");}
                    if((temp1[1].contains("not in"))) { whereCol = temp1[1].split("not in"); Notin = 1;}

                }
                else{
                    Instant timestamp = Instant.now();
                    eventLogs.writeEventLogs
                            ("Error: Invalid query", timestamp, query, DbName, "Update Table");
                    System.out.println("Query is not valid");
                    return false;
                }
                if(whereCol.length != 2){
                    Instant timestamp = Instant.now();
                    eventLogs.writeEventLogs
                            ("Error: Incorrect WHERE condition", timestamp, query, DbName, "Update Table");
                    System.out.println("Problem in the after WHERE condition !");
                    return false;
                }
                if(whereCol[1].contains("(")  || whereCol[1].contains(")")){
                    if(!whereCol[1].contains(")") || !whereCol[1].contains("(")){
                        Instant timestamp = Instant.now();
                        eventLogs.writeEventLogs
                                ("Error: Invalid query", timestamp, query, DbName, "Update Table");
                        System.out.println("Not a valid query!");
                        return false;
                    }
                }
                whereCol[1] = whereCol[1].replaceAll("[()]","");
                String  []RealCol = whereCol[1].split(",");
                for(int i = 0; i  < RealCol.length ; i ++){
                    oldVal.put(RealCol[i].trim(),1);
                }
                return OperationUD(DbName,words[1],whereCol[0].trim(),oldVal,UpdateVal,0,Notin);
            }
    }

    public static boolean CheckDelete(String query,String DbName) throws IOException {
        query= query.trim().replaceAll(" +"," ");

        if(!checkDbExist(DbName)){
            return false;
        }

            query = query.toLowerCase();
            String []words = query.split(" ");
            if(words.length < 3){
                return false;
            }
            if(!words[0].equals("delete") || !words[1].equals("from")){
                Instant timestamp = Instant.now();
                eventLogs.writeEventLogs
                        ("Error: Problem with the keyword - 'Update' or 'Set' ", timestamp, query, DbName, "Delete Table records");
                System.out.println("Problem in the keyword update or set");
                return false;
            }
            if(!CheckTableExist(DbName,words[2].trim())){
                Instant timestamp = Instant.now();
                eventLogs.writeEventLogs
                        ("Error: Table does not exist ", timestamp, query, DbName, "Delete Table");
                System.out.println("Table does not exist");
                return false;
            }

            String TableName = words[2].trim();

            if(words.length == 3){
                OperationUD(DbName,words[2].trim(),"",null,null,1,0);
                    return false;
            }
            else{
                if(!query.contains("where")){
                    Instant timestamp = Instant.now();
                    eventLogs.writeEventLogs
                            ("Error: WHERE clause is missing ", timestamp, query, DbName, "Delete Table records");
                    System.out.println("No where clause");
                    return false;
                }
                else{
                    String [] Where = query.split("where");
                    if(Where.length == 1){
                        Instant timestamp = Instant.now();
                        eventLogs.writeEventLogs
                                ("Error: Incomplete WHERE clause ", timestamp, query, DbName, "Delete Table records");
                        System.out.println("Incomplete Where Condition !");
                        return false;
                    }

                    String AfterWhere = Where[1];
                    int Notin = 1;
                    if(AfterWhere.contains("=") || AfterWhere.contains("!=") ||
                            AfterWhere.contains("in") || AfterWhere.contains("not in")) {

                        String[] ColumnValue;
                        if(AfterWhere.contains("!=")){
                            ColumnValue = AfterWhere.split("!=");

                        }
                        else if (AfterWhere.contains("=")) {
                            ColumnValue = AfterWhere.split("=");
                            Notin = 0;
                        }
                        else if(AfterWhere.contains("not in")){
                            ColumnValue = AfterWhere.split("not in");
                        }
                        else{
                            ColumnValue = AfterWhere.split("in");
                            Notin = 0;

                        }
                        if(ColumnValue.length == 1 || ColumnValue[1].trim().equals("")){
                            Instant timestamp = Instant.now();
                            eventLogs.writeEventLogs
                                    ("Error: Invalid Query After WHERE Clause ", timestamp, query, DbName, "Delete Table records");
                            System.out.println("Invalid Query After Where Clause!");
                            return false;
                        }
                        HashMap<String, Integer> Tcol = new HashMap<>();
                        HashMap<String, Integer> UpdateVal = new HashMap<>();

                        String curPath = DbPath + DbName + "/" + words[2] + "/meta.txt";
                        File f = new File(curPath);
                        BufferedReader bf = new BufferedReader(new FileReader(f));
                        String tableCol = bf.readLine();
                        String[] tmp = tableCol.split(Pattern.quote(seprator));
                        for (Integer i = 0; i < tmp.length; i++) {
                            String [] tmp1 = tmp[i].split(" ");
                            Tcol.put(tmp1[0].trim(), i);
                        }
                        String []TempCol = ColumnValue[0].trim().split(" ");

                        if (!Tcol.containsKey(TempCol[0].trim())) {
                            Instant timestamp = Instant.now();
                            eventLogs.writeEventLogs
                                    ("Error: Column does not exist ", timestamp, query, DbName, "Delete Table records");
                            System.out.println("Column Doesnot Exist");
                            return false;
                        }
                        if(ColumnValue[1].contains("(") || ColumnValue[1].contains(")")){
                            if(!ColumnValue[1].contains("(") || !ColumnValue[1].contains(")")){
                                Instant timestamp = Instant.now();
                                eventLogs.writeEventLogs
                                        ("Error: Invalid Query ", timestamp, query, DbName, "Delete Table records");
                                System.out.println("Not a Valid Query!");
                                return false;
                            }
                        }
                        ColumnValue[1] = ColumnValue[1].replaceAll("[()]", "");
                        String[] Columns = ColumnValue[1].split(",");
                        for(int i = 0; i < Columns.length ; i ++){
                            UpdateVal.put(Columns[i].trim(), 1);
                        }
                        OperationUD(DbName,TableName,TempCol[0].trim(),UpdateVal,null,1,Notin);
                        System.out.println("Values Got Deleted  where column name = " + TempCol[0]  + " and given value");
                    }
                    else{
                        System.out.println("Query is out of scope !");
                        Instant timestamp = Instant.now();
                        eventLogs.writeEventLogs
                                ("Error: Query is out of scope ", timestamp, query, DbName, "Delete Table records");
                        return false;
                    }
                }
            }
        Instant timestamp = Instant.now();
        eventLogs.writeEventLogs
                ("Query executed successfully", timestamp, query, DbName, "Delete Table");
            return true;
        }

    public static void fetch(String DB,String TableName, HashMap<Integer,Integer> SelectColumns,String columnName,HashMap<String,Integer> ColVal , int flag, int Notin) throws IOException {
            String met = "/meta.txt";
            String dat = "/data.txt";
            String curPath = DbPath + DB + "/" + TableName;
            File f = new File(curPath + met);
            BufferedReader bf = new BufferedReader(new FileReader(f));
            String Col = bf.readLine();
            bf.close();
            String []Columns = Col.split(Pattern.quote(seprator));
            int index = -1;
            for(int i = 0; i < Columns.length;i ++){
                String []coltemp = Columns[i].split(" ");
                if(coltemp[0].trim().equals(columnName.trim())){
                    index = i;
                }
            }

            File f1 = new File(curPath + dat);
            BufferedReader bf1 = new BufferedReader(new FileReader(f1));
            String line;
            System.out.println();
            System.out.println("<------------------------------------------------------------------------>");
            while((line = bf1.readLine()) != null) {
                String[] values = line.split(Pattern.quote(seprator));
                if((columnName.equals("") || ColVal.containsKey(values[index].trim()))) {
                    if(Notin == 1){
                        continue;
                    }
                    for (int i = 0; i < values.length; i++) {
                        if (flag == 1 || SelectColumns.containsKey(i)) {
                            System.out.print(values[i] + " ");
                        }
                    }
                    System.out.println();
                }
                else{
                    if (Notin == 1) {
                    for (int i = 0; i < values.length; i++) {
                            if (flag == 1 || SelectColumns.containsKey(i)) {
                                System.out.print(values[i] + " ");
                                }
                        }
                        System.out.println();
                    }
                }
            }
            System.out.println("<------------------------------------------------------------------------>");

            bf1.close();
            return;
        }

    public static HashMap<Integer,Integer> SelectedColumns(String Db , String table ,String query) throws IOException
    {
        HashMap<Integer,Integer> mp1= new HashMap<>();
        String curPath = DbPath + Db + "/" + table + "/meta.txt";
        File f = new File(curPath);
        BufferedReader bf = new BufferedReader(new FileReader(f));
        String line = bf.readLine();
        String []Columns = line.split(Pattern.quote(seprator));
        String [] queryCols = query.split(",");
        int cnt  = query.length() - query.replace(",","").length();
        if(queryCols.length <= cnt){
            Instant timestamp = Instant.now();
            eventLogs.writeEventLogs
                    ("Error: Missing columns ", timestamp, query, Db, "Select query");
            System.out.println("Columns are missing!");
            return null;
        }
        HashMap<String,Integer> AvaiableCols = new HashMap<>();
        for(int  i = 0  ; i < Columns.length ; i ++){
            String []tmp = Columns[i].split(" ");
            AvaiableCols.put(tmp[0].trim(),i);
        }

        for(int i = 0 ; i < queryCols.length ; i ++){
            if(AvaiableCols.containsKey(queryCols[i].trim())){
                mp1.put(AvaiableCols.get(queryCols[i].trim()),1);
            }
            else{
                Instant timestamp = Instant.now();
                eventLogs.writeEventLogs
                        ("Error: Column does not exist", timestamp, query, Db, "Select query");
                System.out.println("Column Doesnot Exist in table!");
                return null;
            }
        }
        return mp1;
    }

    public static String CheckColumnExist(String DbName, String TableName, String query) throws IOException {

        String[] AfterColumns = new String[0];
        if (query.contains("=") || query.contains("in") || query.contains("!=") || query.contains("not in")) {
            if (query.contains("=")) {
                AfterColumns = query.split("=");
            } else if (query.contains("!=")) {
                AfterColumns = query.split("!=");
            } else if (query.contains("in")) {
                AfterColumns = query.split("in");
            } else if (query.contains("not in")) {
                AfterColumns = query.split("not in");
            }
            if(AfterColumns.length == 1){
                Instant timestamp = Instant.now();
                eventLogs.writeEventLogs
                        ("Error: Invalid query", timestamp, query, DbName, "Select query");
                System.out.println("Not a Valid query!");
                return "False";
            }
        }
        else{
            Instant timestamp = Instant.now();
            eventLogs.writeEventLogs
                    ("Error: Invalid query", timestamp, query, DbName, "Select query");
            System.out.println("Not a Valid Query !");
            return "False";
        }
        String[] CurColumn = AfterColumns[0].trim().split(" ");
        String ColumnName = CurColumn[0].trim();
        String curPath = DbPath + DbName + "/" + TableName + "/meta.txt";
        File f = new File(curPath);
        BufferedReader bf = new BufferedReader(new FileReader(f));
        String cols = bf.readLine();
        String[] columns = cols.split(Pattern.quote(seprator));
        for(int  i = 0;  i < columns.length ; i ++){
            if(columns[i].contains(" ")) {
               String[] coltemp = columns[i].split(" ");
               if(coltemp[0].trim().equals(ColumnName.trim())){
                    return ColumnName.trim();
                }
            }
            else if(ColumnName.trim().equals(AfterColumns[i].trim())){
                return  ColumnName.trim();
            }
        }
        Instant timestamp = Instant.now();
        eventLogs.writeEventLogs
                ("Error: Column does not exist", timestamp, query, DbName, "Select query");
        System.out.println("Column Does not Exist!");
        return "False";
    }

    public static HashMap<String,Integer> SendColValues(String query) throws IOException {

        HashMap<String, Integer> val = new HashMap<>();
        String DbName = null;
        if (query.contains("=") || query.contains("in") || query.contains("!=") || query.contains("not in")) {
            String[] Columns = new String[0];
            if (query.contains("=")) {
                Columns = query.split("=");
            } else if (query.contains("!=")) {
                Columns = query.split("!=");
            } else if (query.contains("in")) {
                Columns = query.split("in");
            } else if (query.contains("not in")) {
                Columns = query.split("not in");
            }
            if (Columns.length == 1 || Columns[1].trim().equals("")) {
                return null;
            }
            if (Columns[1].contains("(") || Columns[1].contains(")")) {
                if (!Columns[1].contains(")") || !Columns[1].contains("(")) {
                    Instant timestamp = Instant.now();
                    DbName = null;
                    eventLogs.writeEventLogs
                            ("Error: Invalid query", timestamp, query, DbName, "Select query");
                    System.out.println("Invalid Query!");
                    return null;
                }
            }
            Columns[1] = Columns[1].trim();
            if (Columns[1].contains("(")) {
                Columns[1] = Columns[1].substring(1, Columns[1].length() - 1);
            }
            String[] values = Columns[1].split(",");
            int count = Columns[1].length() - Columns[1].replace(",", "").length();
            if (values.length <= count) {
                return null;
            }
            if (values.length == 0) {
                return null;
            }
            for (int i = 0; i < values.length; i++) {
                String temp = values[i].trim();
                if (temp == "") {
                    return null;
                }
                val.put(values[i].trim(), 1);
            }
            return val;
        } else {
            Instant timestamp = Instant.now();
            eventLogs.writeEventLogs
                    ("Error: Either Query is out of scope or Query is invalid", timestamp, query, DbName, "Select query");
            System.out.println("Either Query is out of scope or Query is invalid!");
            return null;
        }
    }

    public static boolean SelectFromTable(String query,String CurdbName) throws IOException {
        query= query.trim().replaceAll(" +"," ");

        if(User1DB == "None"){
            return false;
        }
        query = query.toLowerCase();
        query = query.trim();
        String []words  =query.split(" ");
        String DbName = null;
        if(!words[0].equals("select") || !query.contains("from") || words.length < 4){
            Instant timestamp = Instant.now();
            eventLogs.writeEventLogs
                    ("Error: Invalid query", timestamp, query, DbName, "Select query");
            System.out.println("Invalid query");
            return false;
        }
         if(words[1].equals("*")) {
             if (!words[2].equals("from")) {
                 Instant timestamp = Instant.now();
                 eventLogs.writeEventLogs
                         ("Error: Invalid query - Expecting FROM after * ", timestamp, query, DbName, "Select query");
                 System.out.println("Query is invalid: Expecting FROM after *");
                 return false;
             } else {
                 if (!CheckTableExist(CurdbName, words[3])) {
                     Instant timestamp = Instant.now();
                     eventLogs.writeEventLogs
                             ("Error: Table does not exist", timestamp, query, DbName, "Select query");
                     System.out.println("Table Doesnot Exist!");
                     return false;
                 }
                 if (words.length == 4) {
                     Instant timestamp = Instant.now();
                     eventLogs.writeEventLogs
                             ("Query executed successfully", timestamp, query, DbName, "Select query");
                     fetch(CurdbName, words[3], null, "", null, 1,0);
                 }
                 else {
                         if (!words[4].equals("where")) {
                             Instant timestamp = Instant.now();
                             eventLogs.writeEventLogs
                                     ("Error: Invalid query - WHERE clause is expected", timestamp, query, DbName, "Select query");
                             System.out.println("Expecting WHERE CLAUSE!");
                             return false;
                         }
                         String[] AfterWhere = query.split("where");
                         if(AfterWhere.length == 1){
                             Instant timestamp = Instant.now();
                             eventLogs.writeEventLogs
                                     ("Error: No Column after WHERE clause", timestamp, query, DbName, "Select query");
                             System.out.println("No Column After where clause!");
                             return false;
                         }
                     String columnName = CheckColumnExist(CurdbName, words[3].trim(), AfterWhere[1]);
                     if(columnName.equals("False")){
                         return false;
                     }

                     var ColumnValues = SendColValues(AfterWhere[1]);
                     if(ColumnValues == null){
                         Instant timestamp = Instant.now();
                         eventLogs.writeEventLogs
                                 ("Error: Column values are not mentioned", timestamp, query, DbName, "Select query");
                         System.out.println("Column Values is not mentioend!");
                         return false;
                     }
                     if (columnName.equals("False")) {
                         Instant timestamp = Instant.now();
                         eventLogs.writeEventLogs
                                 ("Error: Column does not exist", timestamp, query, DbName, "Select query");
                             System.out.println("Column Doesnot Exist!");
                             return false;
                         }
                     int Notin = 0;
                     if(query.contains("not in") || query.contains("!=")){
                         Notin = 1;
                     }
                     Instant timestamp = Instant.now();
                     eventLogs.writeEventLogs
                             ("Query executed successfully", timestamp, query, DbName, "Select query");
                     fetch(CurdbName, words[3], null, columnName, ColumnValues, 1,Notin);
                     }
                 }
             }
         else{
                 String  [] beforeFrom = query.split("from");
                 String[] AfterSelect  = beforeFrom[0].split("select");
                 var selectedColumns = SelectedColumns(CurdbName,words[3],AfterSelect[1].trim());
                 if (selectedColumns == null){
                     return false;
                 }
                 if (words.length == 4) {
                     Instant timestamp = Instant.now();
                     eventLogs.writeEventLogs
                             ("Query executed successfully", timestamp, query, DbName, "Select query");
                     fetch(CurdbName, words[3], selectedColumns, "", null, 0,0);
                    }
                 else {
                     if (!words[4].equals("where")) {
                         Instant timestamp = Instant.now();
                         eventLogs.writeEventLogs
                                 ("Error: Invalid query - WHERE clause is expected", timestamp, query, DbName, "Select query");
                         System.out.println("Expecting WHERE CLAUSE!");
                         return false;
                     }
                     String[] AfterWhere = query.split("where");
                     if(AfterWhere.length == 1){
                         Instant timestamp = Instant.now();
                         eventLogs.writeEventLogs
                                 ("Error: No Column after WHERE clause", timestamp, query, DbName, "Select query");
                         System.out.println("No Column After where clause!");
                         return false;
                     }
                     String columnName = CheckColumnExist(CurdbName, words[3].trim(), AfterWhere[1]);
                     if(columnName.equals("False")){
                         return false;
                     }

                     var ColumnValues = SendColValues(AfterWhere[1]);
                     if(ColumnValues == null){
                         Instant timestamp = Instant.now();
                         eventLogs.writeEventLogs
                                 ("Error: Column values are not mentioned", timestamp, query, DbName, "Select query");
                         System.out.println("Column Values is not mentioend!");
                         return false;
                     }
                     if (columnName.equals("False")) {
                         Instant timestamp = Instant.now();
                         eventLogs.writeEventLogs
                                 ("Error: Column does not exist", timestamp, query, DbName, "Select query");
                         System.out.println("Column Doesnot Exist!");
                         return false;
                     }
                     int Notin = 0;
                     if(query.contains("not in") || query.contains("!=")){
                         Notin = 1;
                     }
                     Instant timestamp = Instant.now();
                     eventLogs.writeEventLogs
                             ("Query executed successfully", timestamp, query, DbName, "Select query");
                     fetch(CurdbName, words[3], selectedColumns, columnName, ColumnValues, 0,Notin);

                 }
         }
         return false;
    }

    public static HashMap<String,Integer> PrimaryKeyValues(String DbName, String TableName) throws IOException {
        HashMap<String,Integer> res = new HashMap<>();
        String curURL = DbPath + DbName + "/" + TableName;
        String Meta = curURL + "/meta.txt";
        String Data  =curURL + "/data.txt";
        int index = -1;
        File f = new File(Meta);
        BufferedReader bf = new BufferedReader(new FileReader(f));
        String columns = bf.readLine();
        String []cols = columns.split(Pattern.quote(seprator));
        for(int i = 0 ; i < cols.length  ; i ++){
            String[] chkPrimary  = cols[i].trim().split(" ");
            if(chkPrimary.length == 2){
                index = i;
                break;
            }
        }
        if(index == -1){
            return null;
        }
        f = new File(Data);
        bf = new BufferedReader(new FileReader(f));
        String line;
        while((line = bf.readLine()) != null)
        {
            String [] Values = line.split(Pattern.quote(seprator));
            res.put(Values[index].trim() , index);
        }
        return res;
    }

    public static boolean CheckInsert(String query , String DbName) throws IOException {
        query= query.trim().replaceAll(" +"," ");
        query = query.trim();
        query = query.toLowerCase();
        String []words =query.split(" ");
        if(!words[0].trim().equals("insert")){
            Instant timestamp = Instant.now();
            eventLogs.writeEventLogs
                    ("Error: Problem with INSERT keyword", timestamp, query, DbName, "Insert query");
            System.out.println("PROBLEM IN INSERT KEYWORD!");
            return false;
        }
        if(!words[1].trim().equals("into")){
            Instant timestamp = Instant.now();
            eventLogs.writeEventLogs
                    ("Error: Incorect INTO keyword", timestamp, query, DbName, "Insert query");
            System.out.println("Wrong into keyword!");
            return false;
        }
        String TableName = words[2].trim();

        if(!CheckTableExist(DbName,words[2].trim())){
            Instant timestamp = Instant.now();
            eventLogs.writeEventLogs
                    ("Error: Table does not exist", timestamp, query, DbName, "Insert query");
            System.out.println("Table does not Exist in the corresponding table!");
            return false;
        }
        String curPath = DbPath  + DbName  + "/" + words[2];
        String meta = curPath + "/meta.txt";
        String data = curPath + "/data.txt";

        var Unique = PrimaryKeyValues(DbName,words[2].trim());
        String [] AfterValues = query.split("values");
        ArrayList<String> queryValues = new ArrayList<>();
        String values = AfterValues[1].trim();
        int i  = 0;
        int open = 0;
        while(i < values.length()){
           String temp = "";
           if(values.charAt(i) == '('){
                open++;
                while(i < values.length()){
                    if(values.charAt(i) == ')'){
                        temp += values.charAt(i++);
                        open--;
                        break;
                    }
                        temp += values.charAt(i++);
                    }
                    if(open == 1){
                        Instant timestamp = Instant.now();
                        eventLogs.writeEventLogs
                                ("Error: Invalid query", timestamp, query, DbName, "Insert query");
                        System.out.println("Not valid Query!");
                        return false;
                    }
                    queryValues.add(temp);
                }
                else{
                    int comma = 0;
                    while(i < values.length() && values.charAt(i) != '('){
                        if(values.charAt(i) == ','){
                            comma ++;
                        }
                        i++;
                    }
                    if(comma != 1){
                        Instant timestamp = Instant.now();
                        eventLogs.writeEventLogs
                                ("Error: Invalid query - Check the query syntax", timestamp, query, DbName, "Insert query");
                        System.out.println(" Not a valid query: Need a comma between Insertion values!");
                    }
                }
            }
            File  f = new File(meta);
            Scanner sc = new Scanner(f);
            String  TCs  = sc.nextLine();
            String  CDt = sc.nextLine();
            sc.close();
            String[] TableColumns = TCs.split(Pattern.quote(seprator));
            String[] ColumnDatatype = CDt.split(Pattern.quote(seprator));
            ArrayList<String> Insert = new ArrayList<>();

            for(String val : queryValues){

                val  = val.replace("(","");
                val = val.replace(")","");
                System.out.println("val : " + val);
                String []colVal = val.split(",");
                if(colVal.length != TableColumns.length){
                    Instant timestamp = Instant.now();
                    eventLogs.writeEventLogs
                            ("Error: Data input is exceeding the number of actual columns", timestamp, query, DbName, "Insert query");
                    System.out.println("The number of DataInput is more than actual Columns!");
                    return false;
                }

                String InsertTmp = "";
                for(int it = 0; it < colVal.length; it ++){
                    String [] TableTmp = TableColumns[it].split(" ");
                    if(TableTmp.length == 2){
                        if(Unique.containsKey(colVal[it].trim()) && Unique.get(colVal[it]) == it){
                            Instant timestamp = Instant.now();
                            eventLogs.writeEventLogs
                                    ("Error: Duplicate values found", timestamp, query, DbName, "Insert query");
                            System.out.println("Duplicate Value found in the Primary key of the table!");
                            return false;
                        }
                    }
                    if(ColumnDatatype[it].trim().equals("int")){
                        if(!isNumeric(colVal[it])){
                            Instant timestamp = Instant.now();
                            eventLogs.writeEventLogs
                                    ("Error: Datatype did not match", timestamp, query, DbName, "Insert query");
                            System.out.println("Datatype didn't match!");
                            return false;
                        }
                        InsertTmp += colVal[it].trim();
                        InsertTmp += seprator;
                    }
                    else{
                        InsertTmp += colVal[it].trim();
                        InsertTmp += seprator;
                    }
                }

                Insert.add(InsertTmp.trim().substring(0, InsertTmp.trim().length()-4));
            }

        File data_dict = new File(DbPath+ DbName + "/data_dictionary.txt");
        BufferedReader read_dict = new BufferedReader(new FileReader(data_dict));
        String line = "";
        String TableCols = "";
        while((line = read_dict.readLine()) != null){
            String[] TableNamesCol = line.split(seprator);
            if(TableNamesCol[0].trim().equals(TableName)){
                TableCols = TableNamesCol[1];
            }
        }
        String []seeColumns = TableCols.split(ColumnSeprator);
        for(int it = 0; it  <seeColumns.length ; it ++){
            String []CheckForeingKey = seeColumns[it].trim().split(" ");
            if(CheckForeingKey[0].equals("FOREIGN_KEY")){

                String[] SplitNow = seeColumns[it].split("FOREIGN_KEY");
                String tableAndCol = SplitNow[1];
                tableAndCol = tableAndCol.replaceAll("[();]","");
                String []temporaryName = tableAndCol.split(",")  ;
                String CheckTableName = temporaryName[0].trim();
                String CheckColumnName = temporaryName[1].trim();

                File ForeignTable = new File(DbPath + DbName + "/" + CheckTableName + "/meta.txt");
                BufferedReader ForeignBuf = new BufferedReader(new FileReader(ForeignTable));

                String ForeignLine = ForeignBuf.readLine();
                String []valuesOfForeignLine = ForeignLine.split(seprator);
                int indexOfPrimary = -1;
                for(i = 0; i < valuesOfForeignLine.length ; i ++){
                    String []ChkForeignCurTable = valuesOfForeignLine[i].split(" ");
                    if(ChkForeignCurTable.length == 2 && ChkForeignCurTable[1].trim().equals("primarykey")){
                        indexOfPrimary = i;
                        break;
                    }
                }

                ForeignBuf = new BufferedReader(new FileReader(DbPath + DbName + "/" + CheckTableName + "/data.txt"));
                HashMap<String, Integer> ForeignTableVal = new HashMap<>();

                while ((line = ForeignBuf.readLine()) != null) {
                    String[] temp = line.split(seprator);
                    ForeignTableVal.put(temp[indexOfPrimary].trim(), 1);
                }


                File CurTable = new File(DbPath + DbName + "/" + TableName + "/meta.txt");
                BufferedReader CurTableBuf = new BufferedReader(new FileReader(CurTable));
                String curTableLine = CurTableBuf.readLine();
                String []valuesOfCurTable = curTableLine.split(seprator);
                int indexOfForeing = -1;
                for(i = 0; i < valuesOfCurTable.length ; i ++) {
                    String[] ChkForeignCurTable = valuesOfCurTable[i].split(" ");

                    if (ChkForeignCurTable.length == 2 && ChkForeignCurTable[1].trim().equals("foreignkey")) {
                        indexOfForeing = i;
                        break;
                    }
                }
                HashMap <String,Integer> CurTableMap  =new HashMap<>();
                for(String s : Insert){
                    String []tmp  = s.split(seprator);
                    CurTableMap.put(tmp[indexOfForeing].trim(), 1);
                }
                for(var j : CurTableMap.keySet()){
                    if(!ForeignTableVal.containsKey(j)){
                        System.out.println("Violates primary key - foreign key relation!");
                        return false;
                    }
                }
            }
        }

            BufferedWriter bw = new BufferedWriter(new FileWriter(data,true));
            for(String s : Insert){
                bw.append(s + "\n");
            }
        bw.close();
        Instant timestamp = Instant.now();
        eventLogs.writeEventLogs
                ("Query executed successfully", timestamp, query, DbName, "Insert query");
        return true;
    }

}








































































