import java.io.IOException;
import java.net.ServerSocket;
import java.io.OutputStream;
import java.io.InputStream;
import java.net.Socket;
import java.util.ArrayList;

class byteArrayManipulation {
  public static int byteArrayToInt(byte[] b) {
    int value  = 0;
    for(byte b1 : b) {
      value = (value << 8) + (b1 & 0xFF);
    }
    return value ;
  }
  public static short byteArrayToShort(byte[] b) {
    int value  = 0;
    for(byte b1 : b) {
      value = (value << 8) + (b1 & 0xFF);
    }
    return (short)value;
  }
  public static byte[] intToByteArray(int data) {    
    byte[] result = new byte[4];
    result[0] = (byte) ((data >> 24) & 0xFF);
    result[1] = (byte) ((data >> 16) & 0xFF);
    result[2] = (byte) ((data >> 8) & 0xFF);
    result[3] = (byte) (data & 0xFF);
    return result;        
}

  public static byte[] shortToByteArray(short d) {
    byte[] result = new byte[2];
    result[0] = (byte) ((d >> 8) & 0xFF); 
    result[1] = (byte) (d & 0xFF);       
    return result;
}
}
public class Main {
  public static void main(String[] args){

    System.err.println("Logs from your program will appear here!");
    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    byteArrayManipulation byteTool = new byteArrayManipulation();
    int port = 9092;

    try {
      serverSocket = new ServerSocket(port);
      // Since the tester restarts your program quite often, setting SO_REUSEADDR
      // ensures that we don't run into 'Address already in use' errors
      serverSocket.setReuseAddress(true);
      // Wait for connection from client.
      clientSocket = serverSocket.accept();
      while(true)
      {
        InputStream inputStream = clientSocket.getInputStream();
        byte[] mssgSize  = new byte[4];
        byte[] apiKey = new byte[2];
        byte[] apiVersion = new byte[2];
        byte[] correlationId = new byte[4];

        if (inputStream.read(mssgSize) == -1) {
          break;  // Client closed connection
        }

        int mssg = byteTool.byteArrayToInt(mssgSize);
        inputStream.read(apiKey);
        short api = byteTool.byteArrayToShort(apiKey);
        inputStream.read(apiVersion);
        short version = byteTool.byteArrayToShort(apiVersion);
        inputStream.read(correlationId);
        int correlation = byteTool.byteArrayToInt(correlationId);

        OutputStream outputStream = clientSocket.getOutputStream();
        ArrayList<byte[]> responses = new ArrayList<>();
        int responseSize = 0;
        System.out.println("correlationId : " + correlation);
        responses.add(byteTool.intToByteArray(correlation));
        responseSize += 4;
        if(version < 0 || version >4){
          responses.add(new byte[]{0,35}); // error code
          responses.add(new byte[]{0,2});
          responses.add(apiKey); //api key
          responses.add(new byte[]{0,0}); // min  version 
          responses.add(new byte[]{0,4}); // max version
          responses.add(new byte[]{0}); // tagged fields api section
          responses.add(new byte[]{0, 0, 0, 0}); // throttle
          responses.add(new byte[]{0}); // tagged fields final section
          responseSize += 16;
        }
        else{
          responses.add(new byte[]{0,0});
          responses.add(new byte[]{2});
          responses.add(new byte[]{0,18}); //api key
          responses.add(new byte[]{0,3}); // min  version 
          responses.add(new byte[]{0,4}); // max version
          responses.add(new byte[]{0}); // tagged fields api section
          responses.add(new byte[]{0, 0, 0, 0}); // throttle
          responses.add(new byte[]{0}); // tagged fields final section

          responseSize += 16;  
        }

        outputStream.write(byteTool.intToByteArray(responseSize));
        for (byte[] response : responses) {
          outputStream.write(response);
        }
        
        outputStream.flush();
    }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        if (clientSocket != null) {
          clientSocket.close();
        }
      } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
      }
    }
  }
}
