import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class KafkaKRaftMetadataParser {
    static void partitioncount(){
        File logDir = new File("/tmp/kraft-combined-logs");
        int partitionCount = 0;

        File[] files = logDir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory() && file.getName().matches(".*-\\d+$")) {
                    partitionCount++;
                    System.out.println("Partition found: " + file.getName());
                }
            }
        }

        System.out.println("Total partitions: " + partitionCount);
    }
    static byteArrayManipulation byteTool = new byteArrayManipulation();
    static void parseMetaProperties(String filePath) {
        System.out.println("== Parsing meta.properties ==");
        try (FileInputStream fis = new FileInputStream(filePath)) {
            Properties props = new Properties();
            props.load(fis);

            props.forEach((key, value) -> System.out.println(key + ": " + value));
        } catch (IOException e) {
            System.err.println("Failed to read meta.properties: " + e.getMessage());
        }
    }
    static int count = 0;
    static void parsePartitionMetadata(String filePath) {
        System.out.println("\n== Parsing partition.metadata ==");
        try {
            byte[] data = Files.readAllBytes(Paths.get(filePath));
            ByteBuffer buffer = ByteBuffer.wrap(data);

            int version = buffer.getShort(); // 2 bytes
            int leaderId = buffer.getInt(); // 4 bytes
            int leaderEpoch = buffer.getInt(); // 4 bytes
            int partitionEpoch = buffer.getInt(); // 4 bytes
            int numReplicas = buffer.getInt(); // 4 bytes

            System.out.println("Version: " + version);
            System.out.println("Leader ID: " + leaderId);
            System.out.println("Leader Epoch: " + leaderEpoch);
            System.out.println("Partition Epoch: " + partitionEpoch);
            System.out.println("Number of Replicas: " + numReplicas);

            System.out.println();

        } catch (IOException e) {
            System.err.println("Failed to read partition.metadata: " + e.getMessage());
        }
    }

    static void parseTopicTopic(byte[] data , LogFileInfo logFileInfo) {
        int ind = 0;
        byte[] version = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;
        byte[] nameLength = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;
        byte[] name = Arrays.copyOfRange(data, ind , ind + byteTool.byteArrayToInt(nameLength)-1);
        ind += byteTool.byteArrayToInt(nameLength)-1;
        byte[] topicUUID  = Arrays.copyOfRange(data, ind , ind + 16);
        ind += 8;
        String TOPIC = new String(Arrays.copyOfRange(name,0,3));
        // System.out.println("========= TOPIC NAME  ========");
        // System.out.println(TOPIC);
        // map.put(TOPIC, topicUUID);
        byte[] taggedFeildCounts = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;

        TopicRecord topicRecord = new TopicRecord();
        topicRecord.version = version;
        topicRecord.nameLength = nameLength;
        topicRecord.nameA = name;
        topicRecord.name = new String(name);
        topicRecord.topicUUID = topicUUID;
        topicRecord.taggedFeildCounts = taggedFeildCounts;
        if(logFileInfo.topics.containsKey(TOPIC)){
            logFileInfo.topics.get(TOPIC).count++;
        }
        else{
            logFileInfo.topics.put(TOPIC, topicRecord);
            logFileInfo.topicUUIDs.put(TOPIC, topicUUID);
            logFileInfo.topicNames.put(byteTool.byteArrayToString(topicUUID) , TOPIC);
        }

    }
    static void pareseTopicFeature(byte[] data) {
        int ind = 0;
        byte[] version = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;
        byte[] nameLength = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;
        byte[] name = Arrays.copyOfRange(data, ind , ind + byteTool.byteArrayToInt(nameLength)-1);
        ind += byteTool.byteArrayToInt(nameLength)-1;
        if(ind >= data.length) return ;
        byte[] featuredLevel = Arrays.copyOfRange(data, ind , ind + 2);
        ind += 2;
        byte[] toggledFeildCounts = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;
    }
    static void parseTopicPartitionHead(byte[] data , LogFileInfo logFileInfo) {
        int ind = 0;
        PartitionRecord partitionRecord = new PartitionRecord();
        byte[] version = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;
        partitionRecord.version = version;

        byte[] partitionID = Arrays.copyOfRange(data, ind , ind + 4);
        ind += 4;
        partitionRecord.partitionID = partitionID;

        byte[] topicUUID = Arrays.copyOfRange(data, ind , ind + 16);
        ind += 16;
        partitionRecord.topicUUID = topicUUID;

        byte[] replicaArrayLength = Arrays.copyOfRange(data, ind , ind + 1);
        ind += 1;
        partitionRecord.replicaArrayLength = replicaArrayLength;

        int lengthOfReplicaArray = byteTool.byteArrayToInt(replicaArrayLength);
        for(int i = 0 ; i < lengthOfReplicaArray ; i++){
            byte[] replicaID = Arrays.copyOfRange(data, ind , ind + 4);
            ind += 4;
            partitionRecord.replicaID.add(replicaID);
        }

        byte[] inSyncReplicaArrayLength = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;
        partitionRecord.inSyncReplicaArrayLength = inSyncReplicaArrayLength;
        int lengthOfInSyncReplicaArray = byteTool.byteArrayToInt(inSyncReplicaArrayLength);
        for(int j = 0 ; j < lengthOfInSyncReplicaArray ; j++){
            byte[] replicaID = Arrays.copyOfRange(data, ind , ind + 4);
            ind += 4;
            partitionRecord.inSyncReplicaID.add(replicaID);
        }

        byte[] removingReplicaArrayLength = new byte[1];
        ind++;
        partitionRecord.lengthOfRemovingReplicaArray = removingReplicaArrayLength;
        int lengthOfRemovingReplicaArray = byteTool.byteArrayToInt(removingReplicaArrayLength);
        for(int k = 0 ; k < lengthOfRemovingReplicaArray ; k++){
            byte[] replicaID = Arrays.copyOfRange(data, ind , ind + 4);
            ind += 4;
            partitionRecord.removingReplicaID.add(replicaID);
        }

        byte[] leader = new byte[4];
        ind += 4;
        partitionRecord.leader = leader;

        byte[] leaderEpoch = new byte[4];
        ind += 4;
        partitionRecord.leaderEpoch = leaderEpoch;

        byte[] partitionEpoch = new byte[4];
        ind += 4;
        partitionRecord.partitionEpoch = partitionEpoch;

        byte[] directoriesArrayLength  = new byte[1];
        ind++;
        partitionRecord.lengthOfDirectoriesArray = directoriesArrayLength;
        for(int i = 1 ; i < byteTool.byteArrayToInt(directoriesArrayLength) ; i++){
            byte[] directoriesUUID = new byte[16];
            ind += 16;
            partitionRecord.directories.add(directoriesUUID);
        }

        byte[] taggedFeildCounts = new byte[1];
        ind++;
        partitionRecord.taggedFeildCounts = taggedFeildCounts;

        if(logFileInfo.topicNames.containsKey(byteTool.byteArrayToString(topicUUID))){
            String topicName = logFileInfo.topicNames.get(byteTool.byteArrayToString(topicUUID));
            logFileInfo.topics.get(topicName).partitions.add(partitionRecord);
        }

    }

    static void parseTopicKeyValue(byte[] data ,int I , LogFileInfo logFileInfo) {
        int ind = 0;
        byte[] frameVersion = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;
        byte[] type = Arrays.copyOfRange(data, ind, ind + 1);
        ind++;

        int typeOfRecord = byteTool.byteArrayToInt(type);
        if(typeOfRecord == 2){

            parseTopicTopic(Arrays.copyOfRange(data, ind, data.length-1),logFileInfo);
        }
        else if(typeOfRecord == 3){
            count++;
            parseTopicPartitionHead(Arrays.copyOfRange(data, ind,data.length-1) , logFileInfo );
        }
        else{
            pareseTopicFeature(Arrays.copyOfRange(data, ind,data.length-1));
        }
        ind = data.length-1;
        byte[] headerArrayCount = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;
    }
    static void pareseTopic(byte[] data,int I ,LogFileInfo logFileInfo) {
        int ind = 0;
        byte[] attributes = Arrays.copyOfRange(data, ind, ind + 1);
        ind++;

        byte[] timeStampDelta = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;

        byte[] offsetDelta = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;

        byte[] keyLength = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;
        
        byte[] value;
        if(byteTool.byteArrayToInt(offsetDelta) > 1){
            value = Arrays.copyOfRange(data, ind , ind + 2);
            ind += 2;
        }
        else{
            value = Arrays.copyOfRange(data, ind , ind + 1);
            ind += 1;
        }
        for(int i = 0 ; i < byteTool.byteArrayToInt(keyLength) ; i++){
            int recordSize = byteTool.byteArrayToInt(Arrays.copyOfRange(value,0,1));

            parseTopicKeyValue(Arrays.copyOfRange(data, ind , ind + recordSize),I, logFileInfo);
        }

        return ;
    }
    static void printWholeLogSegment(String filePath) throws IOException {
        File file = new File(filePath);  // Replace with your file path
        List<String> lines = Files.readAllLines(file.toPath());
        System.out.println("=============MetaData.log===================");
        for (String line : lines) {
            System.out.println(line);
        }
        System.out.println("===========================================");
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            long length = raf.length();  // Total bytes in file
            // System.out.println("Total Bytes: " + length);

            raf.seek(0);  // Go to the start

            // Read byte by byte
            for (long i = 0; i < length; i++) {
                int b = raf.readUnsignedByte();  // Read as unsigned
                // System.out.printf("Byte %05d: 0x%02X%n ", i, b);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    static void parseLogSegment(String filePath , LogFileInfo logFileInfo) {
        // System.out.println("\n== Parsing Kafka log segment ==");
        File file = new File(filePath);
        
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            long fileLength = raf.length();
            int tbu = 0 ; // total byte parsed
            while(tbu < fileLength){
                byte[] batchOffset = new byte[8];
                byte[] batchLength = new byte[4];
                byte[] partitionLeaderEpoch = new byte[4];
                byte[] magicByte = new byte[1];
                byte[] CRC =  new byte[4];
                byte[] attributes = new byte[2];
                byte[] lastOffset = new byte[4];
                byte[] baseTimestamp = new byte[8];
                byte[] maxTimestamp = new byte[8];
                byte[] produerId = new byte[8];
                byte[] producerEpoch = new byte[2];
                byte[] baseSequence = new byte[4];
                byte[] recordLength= new byte[4];
                
                raf.read(batchOffset);
                raf.read(batchLength);

                if(byteTool.byteArrayToInt(batchLength) == 0){
                    // System.out.println(byteTool.byteArrayToInt(batchLength));
                    return ;
                }
                raf.read(partitionLeaderEpoch);
                raf.read(magicByte);
                raf.read(CRC);
                raf.read(attributes);
                raf.read(lastOffset);
                raf.read(baseTimestamp);
                raf.read(maxTimestamp);
                raf.read(produerId);
                raf.read(producerEpoch);
                raf.read(baseSequence);  
                raf.read(recordLength);

                int recordLengthInt = ByteBuffer.wrap(recordLength).getInt();
                tbu += 51;
                for(int i = 0 ; i < recordLengthInt ; i++){
                    byte[] length;
                    if(i == 0) length = new byte[1];
                    else length = new byte[2];    
                    
                    raf.read(length);
                    byteTool.printByteArray(length, "");
                    
                    int sort = byteTool.byteArrayToInt(Arrays.copyOfRange(length, 0, 1));
                    // System.out.println("====== Record " + i + " big indian length =====" );
                    // System.out.println(sort);

                    int recLength = byteTool.zigZagDecode(sort);
                    // System.out.println("====== Record " + i + " zig zag length =====" );
                    // System.out.println(recLength);

                    byte[] content = new byte[recLength];
                    
                    // System.out.println("====== RECORD LENGTH ========");
                    // System.out.println(recLength);   
                    raf.read(content);  
                    if(sort == 0) continue;
                    pareseTopic(content,i,logFileInfo);
                    // System.out.println("***** Record iteration complete ****");
                }
            }
 
        } catch (IOException e) {
            System.err.println("Failed to parse log segment: " + e.getMessage());
        }
        return ;
    }

    public static void parseServerProperties(String path) {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(path)) {
            props.load(fis);
            String port = props.getProperty("server.port");
            String logDirs = props.getProperty("log.dirs");
            String partitions = props.getProperty("num.partitions");
            System.out.println("Server Port: " + port);
            System.out.println("Log Dirs: " + logDirs);
            System.out.println("Partitions: " + partitions);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
