import java.io.IOException;
import java.net.ServerSocket;
import java.io.OutputStream;
import java.io.InputStream;
import java.net.Socket;

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
  public static byte[] intToByteArray( int data ) {    
    byte[] result = new byte[4];
    result[0] = (byte) ((data & 0xFF000000) >> 24);
    result[1] = (byte) ((data & 0x00FF0000) >> 16);
    result[2] = (byte) ((data & 0x0000FF00) >> 8);
    result[3] = (byte) ((data & 0x000000FF) >> 0);
    return result;        
  }
  public static byte[] shortToByteArray(short d) {
    byte[] result = new byte[2];
    result[0] = (byte) ((d >> 8) & 0xFF);  // Higher byte
    result[1] = (byte) (d & 0xFF);         // Lower byte
    return result;
}
}
public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.err.println("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    // 
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
      InputStream inpuiStream = clientSocket.getInputStream();
      byte[] mssgSize  = new byte[4];
      byte[] apiKey = new byte[2];
      byte[] apiVersion = new byte[2];
      byte[] correlationId = new byte[4];

      inpuiStream.read(mssgSize);
      int mssg = byteTool.byteArrayToInt(mssgSize);
      inpuiStream.read(apiKey);
      @SuppressWarnings("unused")
      short api = byteTool.byteArrayToShort(apiKey);
      inpuiStream.read(apiVersion);
      @SuppressWarnings("unused")
      short version = byteTool.byteArrayToShort(apiVersion);
      inpuiStream.read(correlationId);
      int correlation = byteTool.byteArrayToInt(correlationId);

      OutputStream outputStream = clientSocket.getOutputStream();
      outputStream.write(byteTool.intToByteArray(mssg)); // 4-byte message_size
      // outputStream.write(byteTool.shortToByteArray(api)); // 2-byte api_key
      // outputStream.write(byteTool.shortToByteArray(version)); // 2-byte api_version
      outputStream.write(byteTool.intToByteArray(correlation)); // 4-byte correlation_id
      outputStream.write(byteTool.shortToByteArray((short)35));
      outputStream.flush();
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
