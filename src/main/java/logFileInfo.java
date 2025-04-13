import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;

class partitionRecord {
    public partitionRecord() {
        byte[] frameVersion ;
        byte[] type ;
        byte[] version ;
        byte[] partitionID ;
        byte[] topicUUID ;
        byte[] replicaArrayLength ;
        ArrayList<byte[]> replicaID = new ArrayList<byte[]>();
        byte[] inSyncReplicaArrayLength ;
        ArrayList<byte[]> inSyncReplicaID = new ArrayList<byte[]>();
        byte[] lengthOfRemovingReplicaArray;
        ArrayList<byte[]> removingReplicaID = new ArrayList<byte[]>();
        byte[] lengthOfAddingReplicaArray;
        ArrayList<byte[]> addingReplicaID = new ArrayList<byte[]>();
        byte[] leader;
        byte[] leaderEpoch;
        byte[] partitionEpoch;  
        byte[] lengthOfDirectoriesArray;
        ArrayList<byte[]> directories = new ArrayList<byte[]>();
        byte[] taggedFeildCounts;  
    }
}

class 
