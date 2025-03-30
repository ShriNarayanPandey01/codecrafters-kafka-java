import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

public class ApiHandler {
    public static void describePartitionAPI(ArrayList<byte[]> responses , byte[] topicName) {
        responses.add(new byte[]{(byte)0});
        responses.add(new byte[]{0,0,0,0});
        responses.add(new byte[]{2});
        responses.add(new byte[]{0,3});
        responses.add(topicName);
        byte[] nilUuid = new byte[] {
            0x00, 0x00, 0x00, 0x00, 
            0x00, 0x00, 0x00, 0x00, 
            0x00, 0x00, 0x00, 0x00, 
            0x00, 0x00, 0x00, 0x00
        };
        responses.add(nilUuid);
        responses.add(new byte[]{0});
        responses.add(new byte[]{1});
        responses.add(new byte[]{0x00, 0x00, 0x0D, (byte)0xF8});
        responses.add(new byte[]{(byte)0});
        responses.add(new byte[]{(byte)0xff});
        responses.add(new byte[]{(byte)0});

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
