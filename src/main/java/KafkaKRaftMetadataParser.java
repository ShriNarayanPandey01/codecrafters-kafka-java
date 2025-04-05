import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.Properties;

public class KafkaKRaftMetadataParser {
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

            // System.out.print("Replica IDs: ");
            // for (int i = 0; i < numReplicas; i++) {
            //     System.out.print(buffer.getInt() + " ");
            // }
            System.out.println();

        } catch (IOException e) {
            System.err.println("Failed to read partition.metadata: " + e.getMessage());
        }
    }

    static void parseLogSegment(String filePath) {
        System.out.println("\n== Parsing Kafka log segment ==");
        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {
            long fileLength = raf.length();
            while (raf.getFilePointer() < fileLength) {
                long offset = raf.readLong(); // 8 bytes
                int size = raf.readInt(); // 4 bytes
                byte[] recordBatch = new byte[size];
                raf.readFully(recordBatch);

                System.out.printf("Record at offset %d, size %d bytes\n", offset, size);
                // You can parse the recordBatch further here if needed
            }
        } catch (IOException e) {
            System.err.println("Failed to parse log segment: " + e.getMessage());
        }
    }
}
