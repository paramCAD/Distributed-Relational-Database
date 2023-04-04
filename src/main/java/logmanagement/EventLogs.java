package logmanagement;

import support.GlobalData;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Calendar;

public class EventLogs {

    public void writeEventLogs(String msg, Instant timestamp, String query, String databaseName, String queryType) throws IOException {
        boolean fileExists = false;
        if (Files.exists(Paths.get("Event_Logs.txt")))
        {
            fileExists = true;
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter("Event_Logs.txt", true));
        String event_var;
        if(fileExists)
        {
            event_var = "," + "\r\n" +"{" + "\r\n" + "Message: \""  + msg + "\",\r\n" + "timestamp: \"" +timestamp + "\",\r\n" + "query: \""  + query + "\",\r\n" + "user: \"" + GlobalData.userId + "\",\r\n" + "Database name: \"" + databaseName + "\",\r\n" + "QueryType: \"" + queryType + "\",\r\n" +"}";
        }
        else
        {
            event_var = "{ " + "\r\n" + "Message: \""  + msg + "\",\r\n" + "timestamp: \"" +timestamp + "\",\r\n" + "query: \""  + query + "\",\r\n" + "user: \"" + GlobalData.userId + "\",\r\n" + "Database name: \"" + databaseName + "\",\r\n" + "QueryType: \"" + queryType + "\",\r\n" +"}";

        }
        writer.write(event_var);
        writer.close();
    }
}
