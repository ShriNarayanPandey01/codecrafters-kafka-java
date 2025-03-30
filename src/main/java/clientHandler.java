import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;

class ClientHandler extends Thread {
    private Socket clientSocket;

    public ClientHandler(Socket socket) {
        this.clientSocket = socket;
    }

    public void run() {
    byteArrayManipulation byteTool = new byteArrayManipulation();

    try {

      while(true)
      {
        InputStream inputStream = clientSocket.getInputStream();
        OutputStream outputStream = clientSocket.getOutputStream();
        byte[] mssgSize  = new byte[4];
        byte[] apiKey = new byte[2];
        byte[] apiVersion = new byte[2];
        byte[] correlationId = new byte[4];
        byte[] clientLenght , clientId ,remainingBytes ;
        int responseSize;
        ArrayList<byte[]> responses = new ArrayList<>();
        ApiHandler apiHandler = new ApiHandler();
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

        responses.add(correlationId);
        System.out.println(api+"--"+version+"--"+correlation+"--");
        if(api == 75){
          clientLenght = new byte[2];
          inputStream.read(clientLenght);
          clientId = new byte[byteTool.byteArrayToInt(clientLenght)];
          inputStream.read(clientId);
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
          
          // inputStream.read(buffer);
          // System.out.println("got here");
          apiHandler.describePartitionAPI(responses,topicName , topicNameLength);
          responseSize = byteArrayManipulation.sizeOfMessage(responses);
          // remainingBytes = new byte[mssg - 10 + byteTool.byteArrayToInt(clientLenght)];
          // inputStream.read(remainingBytes);
        }
        else{
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
          responseSize = byteArrayManipulation.sizeOfMessage(responses);
          remainingBytes = new byte[mssg - 8];
          inputStream.read(remainingBytes);
        }
        System.out.println(responseSize);
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