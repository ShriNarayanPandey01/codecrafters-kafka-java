import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class KafkaKRaftMetadataParser {

    static byteArrayManipulation byteTool = new byteArrayManipulation();

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

    static void parseTopicTopic(byte[] data, LogFileInfo logFileInfo) {
        int ind = 0;
        // Minor optimization: avoid creating single-element arrays when possible
        byte version = data[ind];
        ind++;
        byte nameLength = data[ind];
        ind++;
        
        int nameLengthInt = byteTool.byteArrayToInt(new byte[]{nameLength});
        byte[] name = Arrays.copyOfRange(data, ind, ind + nameLengthInt - 1);
        ind += nameLengthInt - 1;
        
        byte[] topicUUID = Arrays.copyOfRange(data, ind, ind + 16);
        ind += 8;
        
        // Optimization: avoid creating substring if name is short
        String TOPIC = name.length >= 3 ? new String(Arrays.copyOfRange(name, 0, 3)) : new String(name);
        
        byte taggedFieldCount = data[ind];
        ind++;
        
        TopicRecord topicRecord = new TopicRecord();
        topicRecord.version = new byte[]{version};
        topicRecord.nameLength = new byte[]{nameLength};
        topicRecord.nameA = name;
        topicRecord.name = new String(name);
        topicRecord.topicUUID = topicUUID;
        topicRecord.taggedFeildCounts = new byte[]{taggedFieldCount};
        
        if (logFileInfo.topics.containsKey(TOPIC)) {
            logFileInfo.topics.get(TOPIC).count++;
        } else {
            logFileInfo.topics.put(TOPIC, topicRecord);
            logFileInfo.topicUUIDs.put(TOPIC, topicUUID);
            logFileInfo.topicNames.put(byteTool.byteArrayToString(topicUUID), TOPIC);
        }
    }

    static void pareseTopicFeature(byte[] data) {
        int ind = 0;
        byte[] version = Arrays.copyOfRange(data, ind, ind + 1);
        ind++;
        byte[] nameLength = Arrays.copyOfRange(data, ind, ind + 1);
        ind++;
        byte[] name = Arrays.copyOfRange(data, ind, ind + byteTool.byteArrayToInt(nameLength) - 1);
        ind += byteTool.byteArrayToInt(nameLength) - 1;
        if (ind >= data.length) return;
        byte[] featuredLevel = Arrays.copyOfRange(data, ind, ind + 2);
        ind += 2;
        byte[] toggledFeildCounts = Arrays.copyOfRange(data, ind, ind + 1);
        ind++;
    }

    static void parseTopicPartitionHead(byte[] data, LogFileInfo logFileInfo) {
        int ind = 0;
        PartitionRecord partitionRecord = new PartitionRecord();
        
        byte[] version = Arrays.copyOfRange(data, ind, ind + 1);
        ind++;
        partitionRecord.version = version;

        byte[] partitionID = Arrays.copyOfRange(data, ind, ind + 4);
        ind += 4;
        partitionRecord.partitionID = partitionID;

        byte[] topicUUID = Arrays.copyOfRange(data, ind, ind + 16);
        ind += 16;
        partitionRecord.topicUUID = topicUUID;

        byte[] replicaArrayLength = Arrays.copyOfRange(data, ind, ind + 1);
        ind += 1;
        partitionRecord.replicaArrayLength = replicaArrayLength;

        int lengthOfReplicaArray = byteTool.byteArrayToInt(replicaArrayLength);
        for (int i = 0; i < lengthOfReplicaArray; i++) {
            byte[] replicaID = Arrays.copyOfRange(data, ind, ind + 4);
            ind += 4;
            partitionRecord.replicaID.add(replicaID);
        }

        byte[] inSyncReplicaArrayLength = Arrays.copyOfRange(data, ind, ind + 1);
        ind++;
        partitionRecord.inSyncReplicaArrayLength = inSyncReplicaArrayLength;
        int lengthOfInSyncReplicaArray = byteTool.byteArrayToInt(inSyncReplicaArrayLength);
        for (int j = 0; j < lengthOfInSyncReplicaArray; j++) {
            byte[] replicaID = Arrays.copyOfRange(data, ind, ind + 4);
            ind += 4;
            partitionRecord.inSyncReplicaID.add(replicaID);
        }

        byte[] removingReplicaArrayLength = Arrays.copyOfRange(data, ind, ind + 1);
        ind++;
        partitionRecord.lengthOfRemovingReplicaArray = removingReplicaArrayLength;
        int lengthOfRemovingReplicaArray = byteTool.byteArrayToInt(removingReplicaArrayLength);
        for (int k = 0; k < lengthOfRemovingReplicaArray; k++) {
            byte[] replicaID = Arrays.copyOfRange(data, ind, ind + 4);
            ind += 4;
            partitionRecord.removingReplicaID.add(replicaID);
        }

        byte[] leader = Arrays.copyOfRange(data, ind, ind + 4);
        ind += 4;
        partitionRecord.leader = leader;

        byte[] leaderEpoch = Arrays.copyOfRange(data, ind, ind + 4);
        ind += 4;
        partitionRecord.leaderEpoch = leaderEpoch;

        byte[] partitionEpoch = Arrays.copyOfRange(data, ind, ind + 4);
        ind += 4;
        partitionRecord.partitionEpoch = partitionEpoch;

        byte[] directoriesArrayLength = Arrays.copyOfRange(data, ind, ind + 1);
        ind++;
        partitionRecord.lengthOfDirectoriesArray = directoriesArrayLength;
        for (int i = 1; i < byteTool.byteArrayToInt(directoriesArrayLength); i++) {
            byte[] directoriesUUID = Arrays.copyOfRange(data, ind, ind + 16);
            ind += 16;
            partitionRecord.directories.add(directoriesUUID);
        }

        byte[] taggedFeildCounts = Arrays.copyOfRange(data, ind, ind + 1);
        ind++;
        partitionRecord.taggedFeildCounts = taggedFeildCounts;

        if (logFileInfo.topicNames.containsKey(byteTool.byteArrayToString(topicUUID))) {
            String topicName = logFileInfo.topicNames.get(byteTool.byteArrayToString(topicUUID));
            logFileInfo.topics.get(topicName).partitions.add(partitionRecord);
        }
    }

    static void parseTopicKeyValue(byte[] data, int I, LogFileInfo logFileInfo) {
        int ind = 0;
        byte[] frameVersion = Arrays.copyOfRange(data, ind, ind + 1);
        ind++;
        byte[] type = Arrays.copyOfRange(data, ind, ind + 1);
        ind++;

        int typeOfRecord = byteTool.byteArrayToInt(type);
        if (typeOfRecord == 2) {
            parseTopicTopic(Arrays.copyOfRange(data, ind, data.length - 1), logFileInfo);
        } else if (typeOfRecord == 3) {
            count++;
            parseTopicPartitionHead(Arrays.copyOfRange(data, ind, data.length - 1), logFileInfo);
        } else {
            pareseTopicFeature(Arrays.copyOfRange(data, ind, data.length - 1));
        }
        ind = data.length - 1;
        byte[] headerArrayCount = Arrays.copyOfRange(data, ind, ind + 1);
        ind++;
    }

    static void pareseTopic(byte[] data, int I, LogFileInfo logFileInfo) {
        int ind = 0;
        byte[] attributes = Arrays.copyOfRange(data, ind, ind + 1);
        ind++;

        byte[] timeStampDelta = Arrays.copyOfRange(data, ind, ind + 1);
        ind++;

        byte[] offsetDelta = Arrays.copyOfRange(data, ind, ind + 1);
        ind++;

        byte[] keyLength = Arrays.copyOfRange(data, ind, ind + 1);
        ind++;

        byte[] value;
        if (byteTool.byteArrayToInt(offsetDelta) > 1) {
            value = Arrays.copyOfRange(data, ind, ind + 2);
            ind += 2;
        } else {
            value = Arrays.copyOfRange(data, ind, ind + 1);
            ind += 1;
        }
        
        int keyLengthInt = byteTool.byteArrayToInt(keyLength);
        for (int i = 0; i < keyLengthInt; i++) {
            if (ind >= data.length) break; // Safety check
            
            int recordSize = byteTool.byteArrayToInt(Arrays.copyOfRange(value, 0, 1));
            
            // Safety check for array bounds
            int endIndex = Math.min(ind + recordSize, data.length);
            parseTopicKeyValue(Arrays.copyOfRange(data, ind, endIndex), I, logFileInfo);
            ind = endIndex; // Move index forward
        }
    }

    static void parseLogSegment(String filePath, LogFileInfo logFileInfo) {
        File file = new File(filePath);
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            long fileLength = raf.length();
            long tbu = 0; // total bytes parsed - using long to avoid overflow
            
            // Pre-allocate arrays to avoid repeated allocations
            byte[] batchOffset = new byte[8];
            byte[] batchLength = new byte[4];
            byte[] partitionLeaderEpoch = new byte[4];
            byte[] magicByte = new byte[1];
            byte[] CRC = new byte[4];
            byte[] attributes = new byte[2];
            byte[] lastOffset = new byte[4];
            byte[] baseTimestamp = new byte[8];
            byte[] maxTimestamp = new byte[8];
            byte[] produerId = new byte[8];
            byte[] producerEpoch = new byte[2];
            byte[] baseSequence = new byte[4];
            byte[] recordLength = new byte[4];
            
            while (tbu < fileLength) {
                // Read batch header
                int bytesRead = raf.read(batchOffset);
                if (bytesRead < 8) break;
                
                bytesRead = raf.read(batchLength);
                if (bytesRead < 4) break;

                int batchLengthInt = byteTool.byteArrayToInt(batchLength);
                if (batchLengthInt == 0) {
                    break;
                }
                
                // Read remaining batch header
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
                tbu += 61; // Updated to use correct batch header size
                
                for (int i = 0; i < recordLengthInt && tbu < fileLength; i++) {
                    byte[] length = new byte[i == 0 ? 1 : 2];
                    
                    bytesRead = raf.read(length);
                    if (bytesRead < length.length) break;
                    
                    int sort = byteTool.byteArrayToInt(Arrays.copyOfRange(length, 0, 1));
                    int recLength = byteTool.zigZagDecode(sort);
                    
                    if (recLength <= 0) continue;
                    
                    // Safety check for record length
                    if (tbu + recLength > fileLength) {
                        recLength = (int) (fileLength - tbu);
                    }
                    
                    byte[] content = new byte[recLength];
                    bytesRead = raf.read(content);
                    if (bytesRead < recLength) break;
                    
                    tbu += length.length + recLength;
                    
                    if (sort == 0) continue;
                    pareseTopic(content, i, logFileInfo);
                }
            }

        } catch (IOException e) {
            System.err.println("Failed to parse log segment: " + e.getMessage());
        }
    }
}