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
        ArrayList<byte[]> responses = new ArrayList<>();
        int responseSize = 0;
        byte[] mssgSize  = new byte[4];
        byte[] apiKey = new byte[2];
        byte[] apiVersion = new byte[2];
        byte[] correlationId = new byte[4];
        byte[] clientIdLength , clientId;

        ApiHandler apiHandler = new ApiHandler();

        if (inputStream.read(mssgSize) == -1) {
          break;  // Client closed connection
        }

        int mssg = byteTool.byteArrayToInt(mssgSize);
        inputStream.read(apiKey);
        short api = byteTool.byteArrayToShort(apiKey);
        inputStream.read(apiVersion);
        int version = byteTool.byteArrayToInt(apiVersion);
        inputStream.read(correlationId);
        responses.add(correlationId);
        responseSize += 4;
        if(api == 75){
          clientIdLength = new byte[2];
          inputStream.read(clientIdLength);
          clientId = new byte[byteTool.byteArrayToInt(clientIdLength)];
          inputStream.read(clientId);
          apiHandler.describePartitionHandler(inputStream,mssg,responseSize,responses);
        }
        else{
          if(version < 0 || version >4){
            responses.add(new byte[]{0,35}); // error code
          }
          else{
            responses.add(new byte[]{0,0}); //error code
          }
          apiHandler.apiVersionsHandler(inputStream,mssg,responseSize,responses);
          responseSize += 22;
        }
        
        
        
        // responses.add(new byte[]{3});
        // responses.add(new byte[]{0,18}); //api key
        // responses.add(new byte[]{0,0}); // min  version 
        // responses.add(new byte[]{0,4}); // max version


        // responses.add(new byte[]{(byte)0}); // null


        // responses.add(new byte[]{0,75}); // api key
        // responses.add(new byte[]{0,0}); // min  version 
        // responses.add(new byte[]{0,0}); // max version
    
        
        // responses.add(new byte[]{(byte)0}); //null


        // responses.add(new byte[]{0, 0, 0, 0}); // throttle

        // responses.add(new byte[]{(byte)0});  //null

        // responseSize += 22;  

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