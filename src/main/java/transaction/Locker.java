package transaction;

import java.util.HashMap;

public class Locker {
    HashMap<String, String> locks;

    //Locker Constructor
    Locker(){
        this.locks = new HashMap<String, String>();
    }

    //sets the lock details for a specified user
    Boolean setLock(String user, String database, String tablename){
        if((!this.locks.containsKey(tablename))){
            this.locks.put(tablename, user);
            return true;
        }
        return false;
    }

    //gets the lock details for a specified user
    Boolean releaseLock(String tablename){
        if(this.locks.containsKey(tablename)){
            this.locks.remove(tablename);
            return true;
        }
        return false;
    }
}
