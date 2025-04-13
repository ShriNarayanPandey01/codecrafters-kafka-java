import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

public class ApiHandler {

    static KafkaKRaftMetadataParser parser = new KafkaKRaftMetadataParser();
    static byteArrayManipulation byteTool = new byteArrayManipulation();
    public static void describePartitionAPI(ArrayList<byte[]> responses , byte[] topicName , byte[] topicLength , byte[] topicUUID , byte[] errorCode , byte[] partitionIndex) {
        responses.add(new byte[]{(byte)0}); // tag buffer
        responses.add(new byte[]{0,0,0,0}); // throttle
        responses.add(new byte[]{2}); // ArrayLength   ******
        responses.add(errorCode); // error code
        responses.add(topicLength); // topic length
        responses.add(topicName); // topicName
        responses.add(topicUUID); // topic id
        responses.add(new byte[]{0x00}); // is internal
        responses.add(partitionIndex); // array length 
        for(int i = 1 ; i < 3; i++){
            if(errorCode[1] == 3){   
                // responses.add(partitionIndex); // partition array
                responses.add(new byte[] {0, 0, 0, 0, 1, 1, 0, 1, 1, 1, 1, 1, 1, 0, 0, 0});// topic authorization operation
            } 
            else{
                addPartitionArray(responses , partitionIndex ,errorCode,i);
            }
        }
        responses.add(new byte[]{0x00,0x00,0x0d,(byte)248}); // tag buffer
        responses.add(new byte[]{(byte)0}); // tag buffer
        responses.add(new byte[]{(byte)0xff}); // next cursor
        responses.add(new byte[]{(byte)0}); // tag buffer

    }

    public static void addPartitionArray(ArrayList<byte[]> responses , byte[] partitionIndex , byte[] errorCode , int ind) {
        responses.add(errorCode); // error code
        responses.add(new byte[]{0,0,0,(byte)(ind-1)}); //partition index
        responses.add(new byte[]{0,0,0,(byte)(1)}); // leader id
        responses.add(new byte[]{0,0,0,0}); // leader epoch
        responses.add(new byte[]{(byte)2}); // replica array length
        responses.add(new byte[]{0,0,0,(byte)1}); // replica array content
        responses.add(new byte[]{(byte)2}); // ISR length
        responses.add(new byte[]{0,0,0,(byte)1}); // ISR ARRAY content
        responses.add(new byte[]{(byte)1}); // eligible leader replica
        responses.add(new byte[]{(byte)1}); // last known ELR
        responses.add(new byte[]{(byte)1}); // offline replica
        responses.add(new byte[]{(byte)0}); // buffer
    } 
    public static void apiVersionsHandler(InputStream inputStream, int mssg ,int version ,ArrayList<byte[]> responses){
            if(version < 0 || version >4){
                responses.add(new byte[]{0,35}); // error code
            }
            else{
                responses.add(new byte[]{0,0}); //error code
            }
            responses.add(new byte[]{3});
            responses.add(new byte[]{0,18}); //api key
            responses.add(new byte[]{0,0}); // min  version 
            responses.add(new byte[]{0,4}); // max version
            responses.add(new byte[]{(byte)0}); // null
            responses.add(new byte[]{0,75}); // api key
            responses.add(new byte[]{0,0}); // min  version 
            responses.add(new byte[]{0,0}); // max version
            responses.add(new byte[]{(byte)0}); //null
            responses.add(new byte[]{0, 0, 0, 0}); // throttle
            responses.add(new byte[]{(byte)0});  //null
    }
    public static void searchTopicId(byte[] topicName , byte[] topicLength){
        try{
            FileInputStream fstream = new FileInputStream("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log");
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
            String strLine;
            while ((strLine = br.readLine()) != null)   {
                System.out.println (strLine);
            }
            System.out.println("triggerd the function");
            fstream.close();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
    public static void describePartitionHandler(InputStream inputStream, int mssg ,ArrayList<byte[]> responses){
        try
        {   
            byteArrayManipulation byteTool = new byteArrayManipulation();
            byte[] buffer = new byte[1];
            inputStream.read(buffer);
            byte[] arrayLength = new byte[1];
            inputStream.read(arrayLength);
            byte[] topicNameLength = new byte[1];
            inputStream.read(topicNameLength);
            byte[] topicName = new byte[byteTool.byteArrayToInt(topicNameLength)];
            inputStream.read(topicName);
            inputStream.read(buffer);
            byte[] responsePartitionLimit = new byte[4];
            inputStream.read(responsePartitionLimit);
            byte[] cursor = new byte[1];
            inputStream.read(cursor);
            // parser.parseMetaProperties("/tmp/kraft-combined-logs/meta.properties");
            // parser.parsePartitionMetadata("/tmp/kraft-combined-logs/__cluster_metadata-0/partition.metadata");
            parser.parseServerProperties("/tmp/server.properties");           
            HashMap<String, byte[]> map = parser.parseLogSegment("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log");
            String TOPIC = new String(topicName);
            System.out.println("final topic name "+TOPIC);
            byteTool.printByteArray(topicName);
            // byteTool.printByteArray(map.get(TOPIC));
            for (String key : map.keySet()) {
                byte[] value = map.get(key);
                System.out.println("Key = " + key);
                byteTool.printByteArray(value);
            }
            byte[] topic , errorCode,partitionIndex; 
            if(map.containsKey(TOPIC.substring(0,3))){
                topic = map.get(TOPIC.substring(0,3));
                errorCode = new byte[]{0,0};
                partitionIndex = new byte[]{(byte)2};
            }
            else {
                topic = new byte[15];
                errorCode = new byte[]{0,3};
                partitionIndex = new byte[]{0};
            }
            describePartitionAPI(responses,topicName , topicNameLength , topic , errorCode , partitionIndex);
            
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
