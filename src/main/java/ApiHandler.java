import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class ApiHandler {
    public static void describePartitionAPI(ArrayList<byte[]> responses , byte[] topicName , byte[] topicLength) {
        responses.add(new byte[]{(byte)0}); // tag buffer
        responses.add(new byte[]{0,0,0,0}); // throttle
        responses.add(new byte[]{2}); // ArrayLength   ******
        responses.add(new byte[]{0,0}); // error code
        responses.add(topicLength); // topic length
        responses.add(topicName); // topicName
        byte[] nilUuid = new byte[] {
            0x00, 0x00, 0x00, 0x00, 
            0x00, 0x00, 0x40, 0x00, 
            (byte) 0x80, 0x00, 0x00, 0x00, 
            0x00, 0x00,  0x55
        };
        responses.add(nilUuid); // topic id
        responses.add(new byte[]{0x00}); // is internal
        // responses.add(new byte[]{(byte)2}); // partition array
        // responses.add(new byte[] {0, 0, 0, 0, 1, 1, 0, 1, 1, 1, 1, 1, 1, 0, 0, 0}); // topic authorization operation
        addPartitionArray(responses);
        responses.add(new byte[]{0x00,0x00,0x0d,(byte)248}); // tag buffer
        responses.add(new byte[]{(byte)0}); // tag buffer
        responses.add(new byte[]{(byte)0xff}); // next cursor
        responses.add(new byte[]{(byte)0}); // tag buffer

    }

    public static void addPartitionArray(ArrayList<byte[]> responses){
        responses.add(new byte[]{(byte)2}); // array length 
        responses.add(new byte[]{0,0}); // error code
        responses.add(new byte[]{0,0,0,0}); //partition index
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
            searchTopicId(topicName,topicNameLength);
            describePartitionAPI(responses,topicName , topicNameLength);
            
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
