import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
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
        byte[] remainingBytes = new byte[mssg - 8];
        inputStream.read(remainingBytes);

        OutputStream outputStream = clientSocket.getOutputStream();
        ArrayList<byte[]> responses = new ArrayList<>();
        int responseSize = 0;

        responses.add(correlationId);
        responseSize += 4;
        if(version < 0 || version >4){
          responses.add(new byte[]{0,35}); // error code
        }
        else{
          responses.add(new byte[]{0,0}); //error code
        }
        
        responses.add(new byte[]{2});
        responses.add(new byte[]{0,(byte)(api)}); //api key
        if(api == 18){
            responses.add(new byte[]{0,0}); // min  version 
            responses.add(new byte[]{0,4}); // max version
        }
        else if (api == 75) {
            responses.add(new byte[]{0,0}); // min  version 
            responses.add(new byte[]{0,0}); // max version
        }
        
        responses.add(new byte[]{0}); // tagged fields api section
        responses.add(new byte[]{0, 0, 0, 0}); // throttle
        responses.add(new byte[]{0}); // tagged fields final section

        responseSize += 15;  

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