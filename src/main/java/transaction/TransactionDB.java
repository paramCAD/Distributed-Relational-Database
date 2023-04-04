package transaction;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;

import logmanagement.EventLogs;
import logmanagement.GeneralLogs;
import logmanagement.QueryLogs;

import database.DatabaseHandler;

public class TransactionDB{
    private final List<String> changedFilesList;
    private List<String> currentQueries;
    private TranactionTable table;
    private String database = "";

    private GeneralLogs genLogs = new GeneralLogs();
    private QueryLogs queryLogs = new QueryLogs();
    private EventLogs EventLogs = new EventLogs();

    //costructor of the class
    public TransactionDB(){
        this.changedFilesList =  new ArrayList<String>();
        this.currentQueries = new ArrayList<String>();
        this.table = new TranactionTable(DatabaseHandler.User1DB);
    }

    //checks start of the queries and returns true if query statements has right keywords
    private boolean syntaxChecker(String query){
        query = query.trim();

        //check create related queries
        if(query.contains("create")){
            if(query.contains("table")){
                return true;
            }
            if(query.contains("database")){
                return true;
            }
        }

        //check insert into queries
        if(query.contains("insert") && query.contains("into")){
            if(query.contains("values")){
                return true;
            }
            return false;
        }

        //check delete related queries
        if(query.contains("delete") && query.contains("from")){
            return true;
        }

        //checl the update
        if(query.contains("update")){
            return true;
        }

        //LOGGING FOR SYNTAX FAILURE
        return false;
    }

    //syntax validation and parsing the transaction code block
    private List<Boolean> syntaxValidation(List<String> lines){
        List<Boolean> syntaxCheckList = new ArrayList<Boolean>();

        int numberofLines = lines.size();
        //compares the first line with "start transaction;"
        if(lines.get(0).toLowerCase().equals("start transaction") && (lines.get((numberofLines-1)).toLowerCase().equals("commit") || lines.get((numberofLines-1)).toLowerCase().equals("rollback"))){
            for(int i = 1; i < (numberofLines-1); i++){
                String tempQueryString = lines.get(i).toLowerCase();
                if(syntaxChecker(tempQueryString)){
                    syntaxCheckList.add(Boolean.TRUE);
                    System.out.println(syntaxCheckList);
                }else{
                    syntaxCheckList.add(Boolean.FALSE);
                    System.out.println(syntaxCheckList);

                }

            }
        }
        return syntaxCheckList;
    }

    List<String> removeLine(List<String> lines, int index){
        lines.remove(index);
        return lines;
    }

    //reads queries from the file
    public boolean processTransaction(List<String> lines){

        database = DatabaseHandler.User1DB;
        try {
            for(Boolean syntaxCheckResponse: syntaxValidation(lines)){
                if(syntaxCheckResponse && !database.equals("")){
                    lines = removeLine(lines, 0);
                    for(String query : lines){
                        query = query.trim();
                        String[] keywords = query.toLowerCase().split("\\s+");
                        try{
                            if(keywords[0].equals("create")){
                                if(keywords[1].equals("table")){
                                    long startTime = System.currentTimeMillis();
                                    Instant timestampBefore = Instant.now();
                                    table.createTable(query, database);
                                    Instant timestampAfter = Instant.now();
                                    long endTime = System.currentTimeMillis();
                                    genLogs.writeGeneralLogs(startTime,endTime, timestampBefore, timestampAfter);
                                    queryLogs.writeQueryLogs(query,DatabaseHandler.User1DB);
                                }
                                if(keywords[1].equals("database")){
                                    long startTime = System.currentTimeMillis();
                                    Instant timestampBefore = Instant.now();
                                    table.createDatabase(query);
                                    Instant timestampAfter = Instant.now();
                                    long endTime = System.currentTimeMillis();
                                    genLogs.writeGeneralLogs(startTime,endTime, timestampBefore, timestampAfter);
                                    queryLogs.writeQueryLogs(query,DatabaseHandler.User1DB);
                                }
                            }

                            if(keywords[0].equals("update")){
                                long startTime = System.currentTimeMillis();
                                Instant timestampBefore = Instant.now();
                                table.updateTable(query, database);
                                Instant timestampAfter = Instant.now();
                                long endTime = System.currentTimeMillis();
                                genLogs.writeGeneralLogs(startTime,endTime, timestampBefore, timestampAfter);
                                queryLogs.writeQueryLogs(query,DatabaseHandler.User1DB);
                            }

                            if(keywords[0].equals("insert")){
                                long startTime = System.currentTimeMillis();
                                Instant timestampBefore = Instant.now();
                                table.insertIntoTabel(query, database);
                                Instant timestampAfter = Instant.now();
                                long endTime = System.currentTimeMillis();
                                genLogs.writeGeneralLogs(startTime,endTime, timestampBefore, timestampAfter);
                                queryLogs.writeQueryLogs(query,DatabaseHandler.User1DB);
                            }

                            if(keywords[0].equals("delete")){
                                long startTime = System.currentTimeMillis();
                                Instant timestampBefore = Instant.now();
                                table.deleteFromTable(query, database);
                                Instant timestampAfter = Instant.now();
                                long endTime = System.currentTimeMillis();
                                genLogs.writeGeneralLogs(startTime,endTime, timestampBefore, timestampAfter);
                                queryLogs.writeQueryLogs(query,DatabaseHandler.User1DB);
                            }

                            if(keywords[0].equals("commit")){
                                long startTime = System.currentTimeMillis();
                                Instant timestampBefore = Instant.now();
                                table.commit(lines.toString());
                                Instant timestampAfter = Instant.now();
                                long endTime = System.currentTimeMillis();
                                genLogs.writeGeneralLogs(startTime,endTime, timestampBefore, timestampAfter);
                                queryLogs.writeQueryLogs(query,DatabaseHandler.User1DB);
                            }

                            if(keywords[0].equals("rollback")){
                                long startTime = System.currentTimeMillis();
                                Instant timestampBefore = Instant.now();
                                table.rollback(lines.toString());
                                Instant timestampAfter = Instant.now();
                                long endTime = System.currentTimeMillis();
                                genLogs.writeGeneralLogs(startTime,endTime, timestampBefore, timestampAfter);
                                queryLogs.writeQueryLogs(query,DatabaseHandler.User1DB);
                            }

                        }catch(Exception e){
                            long startTime = System.currentTimeMillis();
                            Instant timestampBefore = Instant.now();
                            table.rollback(lines.toString());
                            Instant timestampAfter = Instant.now();
                            long endTime = System.currentTimeMillis();
                            genLogs.writeGeneralLogs(startTime,endTime, timestampBefore, timestampAfter);
                            queryLogs.writeQueryLogs(query,DatabaseHandler.User1DB);
                        }

                    }
                }
                return true;
            }
        }catch(Exception e){
            //LOG - for not able to read the file
            return false;
        }
        return false;
    }

    // `

}