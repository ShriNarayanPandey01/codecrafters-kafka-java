import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class KafkaKRaftMetadataParser {
    
    // Constants
    private static final int RECORD_TYPE_TOPIC = 2;
    private static final int RECORD_TYPE_PARTITION = 3;
    private static final int BATCH_HEADER_SIZE = 61; // Fixed size of batch header
    private static final int UUID_SIZE = 16;
    private static final int INT_SIZE = 4;
    private static final int LONG_SIZE = 8;
    private static final int SHORT_SIZE = 2;
    
    private static final byteArrayManipulation byteTool = new byteArrayManipulation();
    
    // Utility class for efficient byte buffer operations
    private static class ByteBufferReader {
        private final ByteBuffer buffer;
        private int position;
        
        public ByteBufferReader(byte[] data) {
            this.buffer = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
            this.position = 0;
        }
        
        public byte readByte() {
            if (position >= buffer.limit()) {
                throw new BufferUnderflowException("Attempting to read beyond buffer limit");
            }
            return buffer.get(position++);
        }
        
        public int readInt() {
            if (position + INT_SIZE > buffer.limit()) {
                throw new BufferUnderflowException("Attempting to read beyond buffer limit");
            }
            int value = buffer.getInt(position);
            position += INT_SIZE;
            return value;
        }
        
        public byte[] readBytes(int length) {
            if (position + length > buffer.limit()) {
                throw new BufferUnderflowException("Attempting to read beyond buffer limit");
            }
            byte[] result = new byte[length];
            buffer.get(result, position, length);
            position += length;
            return result;
        }
        
        public void skip(int bytes) {
            position += bytes;
        }
        
        public int getPosition() {
            return position;
        }
        
        public boolean hasRemaining() {
            return position < buffer.limit();
        }
    }

    public static void parseServerProperties(String path) {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(path)) {
            props.load(fis);
            
            // Use getProperty with default values
            String port = props.getProperty("server.port", "9092");
            String logDirs = props.getProperty("log.dirs", "/tmp/kafka-logs");
            String partitions = props.getProperty("num.partitions", "1");
            
            System.out.println("Server Port: " + port);
            System.out.println("Log Dirs: " + logDirs);
            System.out.println("Partitions: " + partitions);

        } catch (IOException e) {
            System.err.println("Failed to read server properties: " + e.getMessage());
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

    static void parsePartitionMetadata(String filePath) {
        System.out.println("\n== Parsing partition.metadata ==");
        try {
            byte[] data = Files.readAllBytes(Paths.get(filePath));
            ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);

            if (buffer.remaining() < 18) { // Minimum required bytes
                System.err.println("Invalid partition.metadata file: insufficient data");
                return;
            }

            int version = buffer.getShort();
            int leaderId = buffer.getInt();
            int leaderEpoch = buffer.getInt();
            int partitionEpoch = buffer.getInt();
            int numReplicas = buffer.getInt();

            System.out.println("Version: " + version);
            System.out.println("Leader ID: " + leaderId);
            System.out.println("Leader Epoch: " + leaderEpoch);
            System.out.println("Partition Epoch: " + partitionEpoch);
            System.out.println("Number of Replicas: " + numReplicas);

        } catch (IOException e) {
            System.err.println("Failed to read partition.metadata: " + e.getMessage());
        }
    }

    static void parseTopicRecord(ByteBufferReader reader, LogFileInfo logFileInfo) {
        try {
            byte version = reader.readByte();
            int nameLength = reader.readByte() & 0xFF; // Unsigned byte
            
            if (nameLength == 0) {
                System.err.println("Invalid topic name length: 0");
                return;
            }
            
            byte[] nameBytes = reader.readBytes(nameLength - 1);
            String topicName = new String(nameBytes);
            
            byte[] topicUUID = reader.readBytes(UUID_SIZE);
            reader.skip(8); // Skip some bytes as per original logic
            
            String topicKey = topicName.length() >= 3 ? topicName.substring(0, 3) : topicName;
            byte taggedFieldCount = reader.readByte();
            
            // Create and populate topic record
            TopicRecord topicRecord = new TopicRecord();
            topicRecord.version = new byte[]{version};
            topicRecord.nameLength = new byte[]{(byte) nameLength};
            topicRecord.nameA = nameBytes;
            topicRecord.name = topicName;
            topicRecord.topicUUID = topicUUID;
            topicRecord.taggedFeildCounts = new byte[]{taggedFieldCount};
            
            // Update logFileInfo
            if (logFileInfo.topics.containsKey(topicKey)) {
                logFileInfo.topics.get(topicKey).count++;
            } else {
                logFileInfo.topics.put(topicKey, topicRecord);
                logFileInfo.topicUUIDs.put(topicKey, topicUUID);
                logFileInfo.topicNames.put(byteTool.byteArrayToString(topicUUID), topicKey);
            }
            
        } catch (Exception e) {
            System.err.println("Error parsing topic record: " + e.getMessage());
        }
    }

    static void parsePartitionRecord(ByteBufferReader reader, LogFileInfo logFileInfo) {
        try {
            PartitionRecord partitionRecord = new PartitionRecord();
            
            byte version = reader.readByte();
            partitionRecord.version = new byte[]{version};
            
            int partitionID = reader.readInt();
            partitionRecord.partitionID = ByteBuffer.allocate(INT_SIZE).putInt(partitionID).array();
            
            byte[] topicUUID = reader.readBytes(UUID_SIZE);
            partitionRecord.topicUUID = topicUUID;
            
            // Read replica arrays
            int replicaArrayLength = reader.readByte() & 0xFF;
            partitionRecord.replicaArrayLength = new byte[]{(byte) replicaArrayLength};
            
            for (int i = 0; i < replicaArrayLength; i++) {
                int replicaID = reader.readInt();
                partitionRecord.replicaID.add(ByteBuffer.allocate(INT_SIZE).putInt(replicaID).array());
            }
            
            // Read in-sync replica arrays
            int inSyncReplicaArrayLength = reader.readByte() & 0xFF;
            partitionRecord.inSyncReplicaArrayLength = new byte[]{(byte) inSyncReplicaArrayLength};
            
            for (int i = 0; i < inSyncReplicaArrayLength; i++) {
                int replicaID = reader.readInt();
                partitionRecord.inSyncReplicaID.add(ByteBuffer.allocate(INT_SIZE).putInt(replicaID).array());
            }
            
            // Read removing replica arrays
            int removingReplicaArrayLength = reader.readByte() & 0xFF;
            partitionRecord.lengthOfRemovingReplicaArray = new byte[]{(byte) removingReplicaArrayLength};
            
            for (int i = 0; i < removingReplicaArrayLength; i++) {
                int replicaID = reader.readInt();
                partitionRecord.removingReplicaID.add(ByteBuffer.allocate(INT_SIZE).putInt(replicaID).array());
            }
            
            // Read other fields
            partitionRecord.leader = reader.readBytes(INT_SIZE);
            partitionRecord.leaderEpoch = reader.readBytes(INT_SIZE);
            partitionRecord.partitionEpoch = reader.readBytes(INT_SIZE);
            
            int directoriesArrayLength = reader.readByte() & 0xFF;
            partitionRecord.lengthOfDirectoriesArray = new byte[]{(byte) directoriesArrayLength};
            
            for (int i = 1; i < directoriesArrayLength; i++) {
                byte[] directoriesUUID = reader.readBytes(UUID_SIZE);
                partitionRecord.directories.add(directoriesUUID);
            }
            
            byte taggedFieldCount = reader.readByte();
            partitionRecord.taggedFeildCounts = new byte[]{taggedFieldCount};
            
            // Associate with topic
            String topicUUIDString = byteTool.byteArrayToString(topicUUID);
            if (logFileInfo.topicNames.containsKey(topicUUIDString)) {
                String topicName = logFileInfo.topicNames.get(topicUUIDString);
                logFileInfo.topics.get(topicName).partitions.add(partitionRecord);
            }
            
        } catch (Exception e) {
            System.err.println("Error parsing partition record: " + e.getMessage());
        }
    }

    static void parseFeatureRecord(ByteBufferReader reader) {
        try {
            byte version = reader.readByte();
            int nameLength = reader.readByte() & 0xFF;
            
            if (nameLength > 0) {
                byte[] name = reader.readBytes(nameLength - 1);
                
                if (reader.hasRemaining()) {
                    byte[] featureLevel = reader.readBytes(SHORT_SIZE);
                    byte taggedFieldCount = reader.readByte();
                }
            }
        } catch (Exception e) {
            System.err.println("Error parsing feature record: " + e.getMessage());
        }
    }

    static void parseKeyValueRecord(byte[] data, int recordIndex, LogFileInfo logFileInfo) {
        if (data.length < 2) {
            System.err.println("Invalid key-value record: insufficient data");
            return;
        }
        
        ByteBufferReader reader = new ByteBufferReader(data);
        
        try {
            byte frameVersion = reader.readByte();
            int recordType = reader.readByte() & 0xFF;
            
            // Create sub-array for the remaining data
            int remainingLength = data.length - reader.getPosition() - 1; // -1 for header array count
            if (remainingLength <= 0) {
                return;
            }
            
            byte[] recordData = reader.readBytes(remainingLength);
            ByteBufferReader recordReader = new ByteBufferReader(recordData);
            
            switch (recordType) {
                case RECORD_TYPE_TOPIC:
                    parseTopicRecord(recordReader, logFileInfo);
                    break;
                case RECORD_TYPE_PARTITION:
                    parsePartitionRecord(recordReader, logFileInfo);
                    break;
                default:
                    parseFeatureRecord(recordReader);
                    break;
            }
            
        } catch (Exception e) {
            System.err.println("Error parsing key-value record: " + e.getMessage());
        }
    }

    static void parseRecord(byte[] data, int recordIndex, LogFileInfo logFileInfo) {
        if (data.length < 4) {
            System.err.println("Invalid record: insufficient data");
            return;
        }
        
        ByteBufferReader reader = new ByteBufferReader(data);
        
        try {
            byte attributes = reader.readByte();
            byte timestampDelta = reader.readByte();
            byte offsetDelta = reader.readByte();
            int keyLength = reader.readByte() & 0xFF;
            
            // Read value length
            int valueLength;
            if ((offsetDelta & 0xFF) > 1) {
                valueLength = reader.readByte() & 0xFF;
                reader.skip(1); // Skip second byte
            } else {
                valueLength = reader.readByte() & 0xFF;
            }
            
            // Process records based on key length
            for (int i = 0; i < keyLength; i++) {
                if (!reader.hasRemaining()) {
                    break;
                }
                
                int recordSize = reader.readByte() & 0xFF;
                
                if (recordSize > 0 && reader.hasRemaining()) {
                    byte[] recordData = reader.readBytes(Math.min(recordSize, 
                        data.length - reader.getPosition()));
                    parseKeyValueRecord(recordData, recordIndex, logFileInfo);
                }
            }
            
        } catch (Exception e) {
            System.err.println("Error parsing record: " + e.getMessage());
        }
    }

    static void parseLogSegment(String filePath, LogFileInfo logFileInfo) {
        File file = new File(filePath);
        
        if (!file.exists() || !file.canRead()) {
            System.err.println("Cannot read log file: " + filePath);
            return;
        }
        
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            long fileLength = raf.length();
            long bytesProcessed = 0;
            
            // Pre-allocate byte arrays for batch header
            byte[] batchHeader = new byte[BATCH_HEADER_SIZE];
            
            while (bytesProcessed < fileLength) {
                // Read batch header
                int headerBytesRead = raf.read(batchHeader);
                if (headerBytesRead < BATCH_HEADER_SIZE) {
                    break;
                }
                
                ByteBuffer headerBuffer = ByteBuffer.wrap(batchHeader).order(ByteOrder.BIG_ENDIAN);
                
                long batchOffset = headerBuffer.getLong();
                int batchLength = headerBuffer.getInt();
                
                if (batchLength == 0) {
                    break;
                }
                
                // Skip other header fields and go to record length
                headerBuffer.position(49); // Skip to record length position
                int recordCount = headerBuffer.getInt();
                
                bytesProcessed += BATCH_HEADER_SIZE;
                
                // Process records
                for (int i = 0; i < recordCount && bytesProcessed < fileLength; i++) {
                    try {
                        // Read record length
                        byte[] lengthBytes = new byte[i == 0 ? 1 : 2];
                        int lengthBytesRead = raf.read(lengthBytes);
                        
                        if (lengthBytesRead < lengthBytes.length) {
                            break;
                        }
                        
                        int encodedLength = lengthBytes[0] & 0xFF;
                        int recordLength = byteTool.zigZagDecode(encodedLength);
                        
                        if (recordLength <= 0) {
                            continue;
                        }
                        
                        // Read record content
                        byte[] recordContent = new byte[recordLength];
                        int contentBytesRead = raf.read(recordContent);
                        
                        if (contentBytesRead < recordLength) {
                            break;
                        }
                        
                        parseRecord(recordContent, i, logFileInfo);
                        bytesProcessed += lengthBytes.length + recordLength;
                        
                    } catch (Exception e) {
                        System.err.println("Error processing record " + i + ": " + e.getMessage());
                        break;
                    }
                }
            }
            
        } catch (IOException e) {
            System.err.println("Failed to parse log segment: " + e.getMessage());
        }
    }
    
    // Helper exception class
    private static class BufferUnderflowException extends RuntimeException {
        public BufferUnderflowException(String message) {
            super(message);
        }
    }
}