package analysis;

public class QueryDto {
    private String db;
    private int count;

    public QueryDto(String db, int count) {
        this.db = db;
        this.count = count;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }
}
