package logmanagement;
import support.GlobalData;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Calendar;
public class QueryLogs {

    public void writeQueryLogs(String query, String dbName) throws IOException
    {
        boolean fileExists = false;
        if (Files.exists(Paths.get("Query_Logs.txt")))
        {
            fileExists = true;
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter("Query_Logs.txt", true));
        String formatted_query;
        String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
        if(fileExists)
        {

            formatted_query = "," + "\r\n" +"{" + "\r\n" + "query: \""  + query + "\",\r\n" + "timestamp: \"" +timestamp + "\",\r\n" + "user: \"" + GlobalData.userId + "\",\r\n" + "db: \"" + dbName + "\",\r\n" + "}";
        }
        else
        {
            formatted_query = "{  " + "\r\n" + "query: \""  + query + "\",\r\n" + "timestamp: \"" +timestamp + "\",\r\n" + "user: \"" + GlobalData.userId + "\",\r\n" + "db: \"" + dbName + "\",\r\n" + "}";
        }
        writer.write(formatted_query);
        writer.close();
    }

}