import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.Arrays;
import java.util.HashMap;
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

    static void parseTopicKeyValue(byte[] data , HashMap<String, Long> map) {
        int ind = 0;
        byte[] frameVersion = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;
        byte[] type = Arrays.copyOfRange(data, ind, ind + 1);
        ind++;
        String  nxtAction = (ByteBuffer.wrap(type).getInt() == 2) ? "topic" : "feature";
        if(nxtAction == "topic"){
            byte[] version = Arrays.copyOfRange(data, ind , ind + 1);
            ind++;
            byte[] nameLength = Arrays.copyOfRange(data, ind , ind + 1);
            ind++;
            byte[] name = Arrays.copyOfRange(data, ind , ind + ByteBuffer.wrap(nameLength).getInt());
            ind += ByteBuffer.wrap(nameLength).getInt();
            byte[] topicUUID  = Arrays.copyOfRange(data, ind , ind + 8);
            ind += 8;
            map.put(ByteBuffer.wrap(name).toString().replace("\0", ""), ByteBuffer.wrap(type).getLong());
            byte[] taggedFeildCounts = Arrays.copyOfRange(data, ind , ind + 1);
            ind++;
        }
        else{
            byte[] version = Arrays.copyOfRange(data, ind , ind + 1);
            ind++;
            byte[] nameLength = Arrays.copyOfRange(data, ind , ind + 1);
            ind++;
            byte[] name = Arrays.copyOfRange(data, ind , ind + ByteBuffer.wrap(nameLength).getInt());
            ind += ByteBuffer.wrap(nameLength).getInt();
            byte[] featuredLevel = Arrays.copyOfRange(data, ind , ind + 2);
            ind += 2;
            byte[] toggledFeildCounts = Arrays.copyOfRange(data, ind , ind + 1);
            ind++;
        }
        }
    static HashMap<String, Long> pareseTopic(byte[] data) {
        HashMap<String, Long> map = new HashMap<>();
        int ind = 0;
        byte[] attributes = Arrays.copyOfRange(data, ind, ind + 1);
        ind++;
        byte[] timeStampDelta = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;
        byte[] offsetDelta = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;
        byte[] keyLength = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;
        for(int i = 0 ; i < ByteBuffer.wrap(keyLength).getInt() ; i++){
            int value = ByteBuffer.wrap(Arrays.copyOfRange(data, ind , ind + 1)).getInt();
            ind++;
            parseTopicKeyValue(Arrays.copyOfRange(data, ind , ind + value), map);
        }

        return map;
    }
    static void parseLogSegment(String filePath) {
        System.out.println("\n== Parsing Kafka log segment ==");
        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {
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
                    byte[] length = new byte[1];
                    
                    raf.read(length);

                    int sort = ByteBuffer.wrap(length).getInt();
                    System.out.println("got to this point yaayyy " + sort);

                    byte[] content = new byte[sort];
                    tbu += sort;
                    raf.read(content);  
                    HashMap<String, Long> map = pareseTopic(content);
                    System.out.print(map.get("foo"));
                }
            }
 
        } catch (IOException e) {
            System.err.println("Failed to parse log segment: " + e.getMessage());
        }
    }
}
