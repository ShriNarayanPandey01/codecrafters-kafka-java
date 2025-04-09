import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

public class KafkaKRaftMetadataParser {
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
        System.out.println("====== frameVersion ======");
        byteTool.printByteArray(frameVersion);
        byte[] type = Arrays.copyOfRange(data, ind, ind + 1);
        ind++;
        System.out.println("====== type ======");
        byteTool.printByteArray(type);

        String  nxtAction = (byteTool.byteArrayToInt(type) == 2) ? "topic" : "feature";
        if(nxtAction == "topic"){
            byte[] version = Arrays.copyOfRange(data, ind , ind + 1);
            ind++;
            System.out.println("====== version ======");
            byteTool.printByteArray(version);
            byte[] nameLength = Arrays.copyOfRange(data, ind , ind + 1);
            ind++;
            System.out.println("====== nameLength ======");
            byteTool.printByteArray(nameLength);
            byte[] name = Arrays.copyOfRange(data, ind , ind + byteTool.byteArrayToInt(nameLength));
            ind += byteTool.byteArrayToInt(nameLength);
            System.out.println("====== name ======");
            byteTool.printByteArray(name);
            byte[] topicUUID  = Arrays.copyOfRange(data, ind , ind + 8);
            ind += 8;
            System.out.println("====== topicUUID ======");
            byteTool.printByteArray(topicUUID);
            // System.out.println(ByteBuffer.wrap(name).toString().replace("\0", ""));
            map.put(ByteBuffer.wrap(name).toString().replace("\0", ""), ByteBuffer.wrap(type).getLong());
            byte[] taggedFeildCounts = Arrays.copyOfRange(data, ind , ind + 1);
            ind++;
            System.out.println("====== taggedFeildCounts ======");
            byteTool.printByteArray(taggedFeildCounts);
        }
        else{
            byte[] version = Arrays.copyOfRange(data, ind , ind + 1);
            ind++;
            System.out.println("====== version ======");
            byteTool.printByteArray(version);
            byte[] nameLength = Arrays.copyOfRange(data, ind , ind + 1);
            ind++;
            System.out.println("====== nameLength ======");
            byteTool.printByteArray(nameLength);
            byte[] name = Arrays.copyOfRange(data, ind , ind + byteTool.byteArrayToInt(nameLength)-1);
            ind += byteTool.byteArrayToInt(nameLength)-1;
            System.out.println("====== name ======");
            byteTool.printByteArray(name);
            byte[] featuredLevel = Arrays.copyOfRange(data, ind , ind + 2);
            ind += 2;
            System.out.println("====== featuredLevel ======");
            byteTool.printByteArray(featuredLevel);
            byte[] toggledFeildCounts = Arrays.copyOfRange(data, ind , ind + 1);
            ind++;
            System.out.println("====== toggledFeildCounts ======");
            byteTool.printByteArray(toggledFeildCounts);
        }
        byte[] headerArrayCount = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;
        System.out.println("====== headerArrayCount ======");
        byteTool.printByteArray(headerArrayCount);    
    }
    static HashMap<String, Long> pareseTopic(byte[] data) {
        HashMap<String, Long> map = new HashMap<>();
        int ind = 0;
        byte[] attributes = Arrays.copyOfRange(data, ind, ind + 1);
        ind++;
        System.out.println("====== attributes ======");
        byteTool.printByteArray(attributes);

        byte[] timeStampDelta = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;
        System.out.println("====== timeStampDelta ======");
        byteTool.printByteArray(timeStampDelta);

        byte[] offsetDelta = Arrays.copyOfRange(data, ind , ind + 1);
        ind++;
        System.out.println("====== offsetDelta ======");
        byteTool.printByteArray(offsetDelta);

        byte[] keyLength = Arrays.copyOfRange(data, ind , ind + 1);
        System.out.println("====== keyLength ======");
        byteTool.printByteArray(keyLength);
        
        ind++;
        for(int i = 0 ; i < byteTool.byteArrayToInt(keyLength) ; i++){
            int value = byteTool.byteArrayToInt(Arrays.copyOfRange(data, ind , ind + 1));
            ind++;
            System.out.println("====== value ======");
            byteTool.printByteArray(Arrays.copyOfRange(data, ind-1 , ind));
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
                System.out.println("====== Batch Offset =====" );
                byteTool.printByteArray(batchOffset);
                raf.read(batchLength);
                System.out.println("====== Batch Length =====" );
                byteTool.printByteArray(batchLength);
                raf.read(partitionLeaderEpoch);
                System.out.println("====== Partition Leader Epoch =====" );
                byteTool.printByteArray(partitionLeaderEpoch);
                raf.read(magicByte);
                System.out.println("====== Magic Byte =====" );
                byteTool.printByteArray(magicByte);
                raf.read(CRC);
                System.out.println("====== CRC =====" );                
                byteTool.printByteArray(CRC);
                raf.read(attributes);
                System.out.println("====== Attributes =====" );
                byteTool.printByteArray(attributes);
                raf.read(lastOffset);
                System.out.println("====== Last Offset =====" );
                byteTool.printByteArray(lastOffset);
                raf.read(baseTimestamp);
                System.out.println("====== Base Timestamp =====" );
                byteTool.printByteArray(baseTimestamp); 
                raf.read(maxTimestamp);
                System.out.println("====== Max Timestamp =====" );
                byteTool.printByteArray(maxTimestamp);
                raf.read(produerId);
                System.out.println("====== Producer Id =====" );
                byteTool.printByteArray(produerId);
                raf.read(producerEpoch);
                System.out.println("====== Producer Epoch =====" );
                byteTool.printByteArray(producerEpoch);
                raf.read(baseSequence);
                System.out.println("====== Base Sequence =====" );
                byteTool.printByteArray(baseSequence);
                raf.read(recordLength);
                System.out.println("====== Record Length =====" );
                byteTool.printByteArray(recordLength);

                int recordLengthInt = ByteBuffer.wrap(recordLength).getInt();
                tbu += 51;
                for(int i = 0 ; i < recordLengthInt ; i++){
                    byte[] length = new byte[1];
                    
                    raf.read(length);
                    System.out.println("====== Record " + i + " Length =====" );
                    byteTool.printByteArray(length);

                    int sort = byteTool.byteArrayToInt(length);
                    int recorLength = byteTool.zigZagDecode(sort)
                    byte[] content = new byte[recorLength];
                    
                    System.out.println("====== RECORD LENGTH >>"+sort );
                    raf.read(content);  
                    System.out.println("====== Record " + i + " Content =====" );
                    byteTool.printByteArray(content);

                    if(sort == 0) continue;
                    HashMap<String, Long> map = pareseTopic(content);
                    System.out.print(map.get("foo"));
                    byte[] headerArrayCount = new byte[1];
                    raf.read(headerArrayCount);
                    tbu += 1;
                }
            }
 
        } catch (IOException e) {
            System.err.println("Failed to parse log segment: " + e.getMessage());
        }
    }
}
