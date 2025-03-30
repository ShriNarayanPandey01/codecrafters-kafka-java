import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

public class ApiHandler {
    public static void describePartitionAPI(ArrayList<byte[]> responses , byte[] topicName , byte[] topicLength) {
        responses.add(new byte[]{(byte)0}); // tag buffer
        responses.add(new byte[]{0,0,0,0}); // throttle
        responses.add(new byte[]{2}); // ArrayLength   ******
        responses.add(new byte[]{0,3}); // error code
        responses.add(topicLength); // topic length
        responses.add(topicName); // topicName
        byte[] nilUuid = new byte[] {
            0x00, 0x00, 0x00, 0x00, 
            0x00, 0x00, 0x00, 0x00, 
            0x00, 0x00, 0x00, 0x00, 
            0x00, 0x00, 0x00, 0x00
        };
        responses.add(nilUuid); // topic id
        responses.add(new byte[]{(byte)0}); // is internal
        responses.add(new byte[]{(byte)2}); // partition array
        responses.add(new byte[] {0, 0, 0, 0, 1, 1, 0, 1, 1, 1, 1, 1, 1, 0, 0, 0}); // topic authorization operation
        responses.add(new byte[]{(byte)0}); // tag buffer
        responses.add(new byte[]{(byte)0xff}); // next cursor
        responses.add(new byte[]{(byte)0}); // tag buffer

    }
    public static void apiVersionsHandler(InputStream inputStream, int mssg ,ArrayList<byte[]> responses){
     
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
    // public static void describePartitionHandler(InputStream inputStream, int mssg ,ArrayList<byte[]> responses){
    //     try
    //     {   
    //         byteArrayManipulation byteTool = new byteArrayManipulation();
            

    //     }
    //     catch (IOException e) {
    //         e.printStackTrace();
    //     }
    // }
}
