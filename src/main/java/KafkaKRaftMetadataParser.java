import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
        byteTool.printByteArray(version , "Version");
        byte[] nameLength = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;
        byteTool.printByteArray(nameLength , "Name Length");
        byte[] name = Arrays.copyOfRange(data, ind , ind + byteTool.byteArrayToInt(nameLength)-1);
        ind += byteTool.byteArrayToInt(nameLength)-1;
        byteTool.printByteArray(name , "Name");
        byte[] topicUUID  = Arrays.copyOfRange(data, ind , ind + 16);
        ind += 8;
        byteTool.printByteArray(topicUUID , "topicUUID");
        String TOPIC = new String(Arrays.copyOfRange(name,0,3));
        System.out.println("========= TOPIC NAME  ========");
        System.out.println(TOPIC);
        // map.put(TOPIC, topicUUID);
        byte[] taggedFeildCounts = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;
        byteTool.printByteArray(taggedFeildCounts , "taggedFeildCounts");

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
        byteTool.printByteArray(version , "Version");
        byte[] nameLength = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;
        byteTool.printByteArray(nameLength , "Name Length");
        byte[] name = Arrays.copyOfRange(data, ind , ind + byteTool.byteArrayToInt(nameLength)-1);
        ind += byteTool.byteArrayToInt(nameLength)-1;
        byteTool.printByteArray(name , "Name");
        if(ind >= data.length) return ;
        byte[] featuredLevel = Arrays.copyOfRange(data, ind , ind + 2);
        ind += 2;
        byteTool.printByteArray(featuredLevel , "Featured Level");
        byte[] toggledFeildCounts = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;
        byteTool.printByteArray(toggledFeildCounts , "Toggled FeildCounts");
    }
    static void parseTopicPartitionHead(byte[] data , LogFileInfo logFileInfo) {
        int ind = 0;
        PartitionRecord partitionRecord = new PartitionRecord();
        byte[] version = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;
        partitionRecord.version = version;
        byteTool.printByteArray(version , "Version");

        byte[] partitionID = Arrays.copyOfRange(data, ind , ind + 4);
        ind += 4;
        partitionRecord.partitionID = partitionID;
        byteTool.printByteArray(partitionID , "Partition ID");

        byte[] topicUUID = Arrays.copyOfRange(data, ind , ind + 16);
        ind += 16;
        partitionRecord.topicUUID = topicUUID;
        byteTool.printByteArray(topicUUID , "Topic UUID");

        byte[] replicaArrayLength = Arrays.copyOfRange(data, ind , ind + 1);
        ind += 1;
        partitionRecord.replicaArrayLength = replicaArrayLength;
        byteTool.printByteArray(replicaArrayLength , "Replica Array Length");

        int lengthOfReplicaArray = byteTool.byteArrayToInt(replicaArrayLength);
        for(int i = 0 ; i < lengthOfReplicaArray ; i++){
            byte[] replicaID = Arrays.copyOfRange(data, ind , ind + 4);
            ind += 4;
            partitionRecord.replicaID.add(replicaID);
            byteTool.printByteArray(replicaID , "Replica ID");
        }

        byte[] inSyncReplicaArrayLength = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;
        partitionRecord.inSyncReplicaArrayLength = inSyncReplicaArrayLength;
        byteTool.printByteArray(inSyncReplicaArrayLength , "In Sync Replica Array Length");
        int lengthOfInSyncReplicaArray = byteTool.byteArrayToInt(inSyncReplicaArrayLength);
        for(int j = 0 ; j < lengthOfInSyncReplicaArray ; j++){
            byte[] replicaID = Arrays.copyOfRange(data, ind , ind + 4);
            ind += 4;
            partitionRecord.inSyncReplicaID.add(replicaID);
            byteTool.printByteArray(replicaID , "In Sync Replica ID");
        }

        byte[] removingReplicaArrayLength = new byte[1];
        ind++;
        partitionRecord.lengthOfRemovingReplicaArray = removingReplicaArrayLength;
        byteTool.printByteArray(removingReplicaArrayLength , "Removing Replica Array Length");
        int lengthOfRemovingReplicaArray = byteTool.byteArrayToInt(removingReplicaArrayLength);
        for(int k = 0 ; k < lengthOfRemovingReplicaArray ; k++){
            byte[] replicaID = Arrays.copyOfRange(data, ind , ind + 4);
            ind += 4;
            partitionRecord.removingReplicaID.add(replicaID);
            byteTool.printByteArray(replicaID , "Removing Replica ID");        
        }

        byte[] leader = new byte[4];
        ind += 4;
        partitionRecord.leader = leader;
        byteTool.printByteArray(leader , "Leader");

        byte[] leaderEpoch = new byte[4];
        ind += 4;
        partitionRecord.leaderEpoch = leaderEpoch;
        byteTool.printByteArray(leaderEpoch , "Leader Epoch");

        byte[] partitionEpoch = new byte[4];
        ind += 4;
        partitionRecord.partitionEpoch = partitionEpoch;
        byteTool.printByteArray(partitionEpoch , "Partition Epoch");    

        byte[] directoriesArrayLength  = new byte[1];
        ind++;
        partitionRecord.lengthOfDirectoriesArray = directoriesArrayLength;
        byteTool.printByteArray(directoriesArrayLength , "Directories Array Length");
        for(int i = 1 ; i < byteTool.byteArrayToInt(directoriesArrayLength) ; i++){
            byte[] directoriesUUID = new byte[16];
            ind += 16;
            partitionRecord.directories.add(directoriesUUID);
            byteTool.printByteArray(directoriesUUID , "Directories UUID");
        }

        byte[] taggedFeildCounts = new byte[1];
        ind++;
        partitionRecord.taggedFeildCounts = taggedFeildCounts;
        byteTool.printByteArray(taggedFeildCounts , "Tagged Feild Counts");

        if(logFileInfo.topicNames.containsKey(byteTool.byteArrayToString(topicUUID))){
            String topicName = logFileInfo.topicNames.get(byteTool.byteArrayToString(topicUUID));
            logFileInfo.topics.get(topicName).partitions.add(partitionRecord);
        }

    }

    static void parseTopicKeyValue(byte[] data ,int I , LogFileInfo logFileInfo) {
        int ind = 0;
        byte[] frameVersion = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;
        byteTool.printByteArray(frameVersion , "Frame Version");
        byte[] type = Arrays.copyOfRange(data, ind, ind + 1);
        ind++;
        byteTool.printByteArray(type , "Type");

        int typeOfRecord = byteTool.byteArrayToInt(type);
        if(typeOfRecord == 2){

            parseTopicTopic(Arrays.copyOfRange(data, ind, data.length-1),logFileInfo);
        }
        else if(typeOfRecord == 3){
            count++;
            parseTopicPartitionHead(Arrays.copyOfRange(data, ind,data.length-1) , logFileInfo );
        }
        else{
            // System.out.println(ind+" "+(data.length-1));
            pareseTopicFeature(Arrays.copyOfRange(data, ind,data.length-1));
        }
        ind = data.length-1;
        byte[] headerArrayCount = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;
        byteTool.printByteArray(headerArrayCount , "Header Array Count");    
    }
    static void pareseTopic(byte[] data,int I ,LogFileInfo logFileInfo) {
        int ind = 0;
        byte[] attributes = Arrays.copyOfRange(data, ind, ind + 1);
        ind++;
        byteTool.printByteArray(attributes , "Attributes");

        byte[] timeStampDelta = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;
        byteTool.printByteArray(timeStampDelta, "timeStampDelta");

        byte[] offsetDelta = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;
        byteTool.printByteArray(offsetDelta, "offsetDelta");

        byte[] keyLength = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;
        byteTool.printByteArray(keyLength , "keyLength");
        
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

            byteTool.printByteArray(value , "Value");
            parseTopicKeyValue(Arrays.copyOfRange(data, ind , ind + recordSize),I, logFileInfo);
        }
        System.out.println("============no of partition here =========");
        System.out.println(count);

        return ;
    }
    static void printWholeLogSegment(String filePath) {
        File file = new File(filePath);  // Replace with your file path

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            long length = raf.length();  // Total bytes in file
            System.out.println("Total Bytes: " + length);

            raf.seek(0);  // Go to the start

            // Read byte by byte
            for (long i = 0; i < length; i++) {
                int b = raf.readUnsignedByte();  // Read as unsigned
                System.out.printf("Byte %05d: 0x%02X%n ", i, b);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    static void parseLogSegment(String filePath , LogFileInfo logFileInfo) {
        System.out.println("\n== Parsing Kafka log segment ==");
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
                byteTool.printByteArray(batchOffset , "Batch Offset");
                raf.read(batchLength);
                byteTool.printByteArray(batchLength , "Batch Length");

                if(byteTool.byteArrayToInt(batchLength) == 0){
                    System.out.println(byteTool.byteArrayToInt(batchLength));
                    return ;
                }
                raf.read(partitionLeaderEpoch);


                byteTool.printByteArray(partitionLeaderEpoch , "Partition Leader Epoch");
                raf.read(magicByte);
                byteTool.printByteArray(magicByte , "Magic Byte");
                raf.read(CRC);
                byteTool.printByteArray(CRC , "CRC");
                raf.read(attributes);
                byteTool.printByteArray(attributes,"Attributes");
                raf.read(lastOffset);
                byteTool.printByteArray(lastOffset,"Last Offset");
                raf.read(baseTimestamp);
                byteTool.printByteArray(baseTimestamp , "Base Timestamp"); 
                raf.read(maxTimestamp);
                byteTool.printByteArray(maxTimestamp, "Max Timestamp");
                raf.read(produerId);
                byteTool.printByteArray(produerId , "Producer Id");
                raf.read(producerEpoch);
                byteTool.printByteArray(producerEpoch , "Producer Epoch");
                raf.read(baseSequence);  
                byteTool.printByteArray(baseSequence , "Base Sequence");
                raf.read(recordLength);
                byteTool.printByteArray(recordLength , "Record Length");

                int recordLengthInt = ByteBuffer.wrap(recordLength).getInt();
                tbu += 51;
                for(int i = 0 ; i < recordLengthInt ; i++){
                    byte[] length;
                    if(i == 0) length = new byte[1];
                    else length = new byte[2];    
                    
                    raf.read(length);
                    byteTool.printByteArray(length,"Record " + i + " Length" );
                    
                    int sort = byteTool.byteArrayToInt(Arrays.copyOfRange(length, 0, 1));
                    System.out.println("====== Record " + i + " big indian length =====" );
                    System.out.println(sort);

                    int recLength = byteTool.zigZagDecode(sort);
                    System.out.println("====== Record " + i + " zig zag length =====" );
                    System.out.println(recLength);

                    byte[] content = new byte[recLength];
                    
                    System.out.println("====== RECORD LENGTH ========");
                    System.out.println(recLength);   
                    raf.read(content);  
                    if(sort == 0) continue;
                    pareseTopic(content,i,logFileInfo);
                    System.out.println("***** Record iteration complete ****");
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
