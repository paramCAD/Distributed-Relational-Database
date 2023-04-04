package analysis;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class Queries {
    private String filePath = "";
    private File file = new File(filePath + "Query_Logs.txt");

    private Map<String, QueryDto> countQuery() throws IOException {
        String json = Files.readString(Paths.get(filePath + file));
        String[] arrayOfJson= json.split("}");
        Map<String, QueryDto> map = new HashMap<>();

        for(String singleJson: arrayOfJson) {
            String[] arrayOfProperties = singleJson.split("\",");
            String user = arrayOfProperties[2].substring(arrayOfProperties[2].indexOf(":"));
            String db = arrayOfProperties[3].substring(arrayOfProperties[3].indexOf(":"));
            int count;
            if (map.get(user) == null) {
                count = 1;
            } else {
                count = map.get(user).getCount() + 1;
            }
            map.put(user, new QueryDto(db,count));
        }
        return map;
    }

    public void printCountQuery() throws IOException {
        Map<String, QueryDto> map = countQuery();
        for (Map.Entry<String,QueryDto> entry : map.entrySet()) {
            String formattedName = entry.getKey().replaceAll("[^a-zA-Z0-9]", "");
            String formattedDbName = entry.getValue().getDb().replaceAll("[^a-zA-Z0-9]", "");
            int machine;
            if(formattedDbName.contains("1")) {
                machine = 1;
            } else {
                machine = 2;
            }

            System.out.println("user " + formattedName + " submitted " + entry.getValue().getCount() + " queries for " + formattedDbName + " running on Virtual Machine " + machine) ;
        }
    }

    private Map<String, Integer> countUpdate() throws IOException {
        String json = Files.readString(Paths.get(filePath + file));
        String[] arrayOfJson= json.split("}");
        Map<String, Integer> map = new HashMap<>();

        for(String singleJson: arrayOfJson) {
            String[] arrayOfProperties = singleJson.split("\",");
            String query = arrayOfProperties[0].substring(arrayOfProperties[0].indexOf(":"));
            String[] queryParts = query.split(" ");
            if(query.contains("update")) {
                String table = queryParts[2];
                int count;
                if(map.get(table) == null) {
                    count = 1;
                } else {
                    count = map.get(table) + 1;
                }
                map.put(table, count);
            }
        }
        return map;
    }

    private Map<String, Integer> countInsert() throws IOException {
        String json = Files.readString(Paths.get(filePath + file));
        String[] arrayOfJson= json.split("}");
        Map<String, Integer> map = new HashMap<>();

        for(String singleJson: arrayOfJson) {
            String[] arrayOfProperties = singleJson.split("\",");
            String query = arrayOfProperties[0].substring(arrayOfProperties[0].indexOf(":"));
            String[] queryParts = query.split(" ");
            if(query.contains("insert")) {
                String table = queryParts[3];
                int count;
                if(map.get(table) == null) {
                    count = 1;
                } else {
                    count = map.get(table) + 1;
                }
                map.put(table, count);
            }
        }
        return map;
    }

    private Map<String, Integer> countDelete() throws IOException {
        String json = Files.readString(Paths.get(filePath + file));
        String[] arrayOfJson= json.split("}");
        Map<String, Integer> map = new HashMap<>();

        for(String singleJson: arrayOfJson) {
            String[] arrayOfProperties = singleJson.split("\",");
            String query = arrayOfProperties[0].substring(arrayOfProperties[0].indexOf(":"));
            String[] queryParts = query.split(" ");
            if(query.contains("delete")) {
                String table = queryParts[3];
                int count;
                if(map.get(table) == null) {
                    count = 1;
                } else {
                    count = map.get(table) + 1;
                }
                map.put(table, count);
            }
        }
        return map;
    }

    public void printCountUpdate() throws IOException {
        Map<String, Integer> map = countUpdate();
        for (Map.Entry<String,Integer> entry : map.entrySet()) {
            System.out.println("Total " + entry.getValue() + " Update operations are performed on " + entry.getKey());
        }
    }

    public void printCountInsert() throws IOException {
        Map<String, Integer> map = countInsert();
        for (Map.Entry<String,Integer> entry : map.entrySet()) {
            System.out.println("Total " + entry.getValue() + " Insert operations are performed on " + entry.getKey());
        }
    }


    public void printCountDelete() throws IOException {
        Map<String, Integer> map = countDelete();
        for (Map.Entry<String,Integer> entry : map.entrySet()) {
            System.out.println("Total " + entry.getValue() + " Delete operations are performed on " + entry.getKey());
        }
    }
}
