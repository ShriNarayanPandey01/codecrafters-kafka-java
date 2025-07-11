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
    static void printWholeLogSegment(String filePath){
        File file = new File(filePath);  // Replace with your file path
        
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
    static void parseLogSegment(String filePath, LogFileInfo logFileInfo) throws IOException {
    File file = new File(filePath);
    
    // Debug: Print file hex dump
    System.out.println("=== File Hex Dump (first 256 bytes) ===");
    try (FileInputStream fis = new FileInputStream(file)) {
        byte[] preview = new byte[Math.min(256, (int)file.length())];
        fis.read(preview);
        for (int i = 0; i < preview.length; i++) {
            if (i % 16 == 0) System.out.printf("\n%04X: ", i);
            System.out.printf("%02X ", preview[i] & 0xFF);
        }
        System.out.println("\n");
    }

    System.out.println("==================");
    
    try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
        long fileLength = raf.length();
        long position = 0;
        
        while (position < fileLength) {
            // Save current position
            long batchStartPosition = raf.getFilePointer();
            
            // Read batch header
            byte[] batchOffset = new byte[8];
            byte[] batchLength = new byte[4];
            
            if (raf.read(batchOffset) != 8 || raf.read(batchLength) != 4) {
                break; // End of file
            }
            
            int batchLengthInt = ByteBuffer.wrap(batchLength).getInt();
            if (batchLengthInt <= 0) {
                System.out.println("Invalid batch length: " + batchLengthInt);
                break;
            }
            
            // Read rest of batch header
            byte[] partitionLeaderEpoch = new byte[4];
            byte[] magicByte = new byte[1];
            byte[] crc = new byte[4];
            byte[] attributes = new byte[2];
            byte[] lastOffsetDelta = new byte[4];
            byte[] baseTimestamp = new byte[8];
            byte[] maxTimestamp = new byte[8];
            byte[] producerId = new byte[8];
            byte[] producerEpoch = new byte[2];
            byte[] baseSequence = new byte[4];
            byte[] recordCount = new byte[4];
            
            readFully(raf, partitionLeaderEpoch);
            readFully(raf, magicByte);
            readFully(raf, crc);
            readFully(raf, attributes);
            readFully(raf, lastOffsetDelta);
            readFully(raf, baseTimestamp);
            readFully(raf, maxTimestamp);
            readFully(raf, producerId);
            readFully(raf, producerEpoch);
            readFully(raf, baseSequence);
            readFully(raf, recordCount);
            
            int recordCountInt = ByteBuffer.wrap(recordCount).getInt();
            System.out.println("Batch at offset " + ByteBuffer.wrap(batchOffset).getLong() + 
                             " has " + recordCountInt + " records");
            
            // Parse records
            for (int i = 0; i < recordCountInt; i++) {
                try {
                    // Read record length (varint encoding)
                    int recordLength = readVarint(raf);
                    if (recordLength <= 0) continue;
                    
                    byte[] recordData = new byte[recordLength];
                    readFully(raf, recordData);
                    
                    // Parse the record
                    parseRecord(recordData, i, logFileInfo);
                    
                } catch (Exception e) {
                    System.err.println("Error parsing record " + i + ": " + e.getMessage());
                    break;
                }
            }
            
            // Move to next batch
            position = batchStartPosition + 12 + batchLengthInt;
            raf.seek(position);
        }
    }
}

    private static void readFully(RandomAccessFile raf, byte[] buffer) throws IOException {
        int totalRead = 0;
        while (totalRead < buffer.length) {
            int read = raf.read(buffer, totalRead, buffer.length - totalRead);
            if (read == -1) throw new EOFException();
            totalRead += read;
        }
    }

    private static int readVarint(RandomAccessFile raf) throws IOException {
        int value = 0;
        int shift = 0;
        byte b;
        do {
            b = raf.readByte();
            value |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return value;
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
