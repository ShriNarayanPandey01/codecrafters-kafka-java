import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.util.ArrayList;

class ClientHandler extends Thread {
    private Socket clientSocket;

    public ClientHandler(Socket socket) {
        this.clientSocket = socket;
    }
    static KafkaKRaftMetadataParser parser = new KafkaKRaftMetadataParser();
    public void run() {
    byteArrayManipulation byteTool = new byteArrayManipulation();

    try {
      while(true)
      {

        LogFileInfo logfile = new LogFileInfo();
           
      //   File rootDir = new File("/tmp/kraft-combined-logs/");
      //   ArrayList<File> logFiles = new ArrayList<>();
      //   Files.walk(rootDir.toPath())
      //   .filter(path -> path.toFile().isFile() && path.toString().endsWith(".log"))
      //   .forEach(path -> logFiles.add(path.toFile()));

      //   for (File logFile : logFiles) {
      //     System.out.println("Parsing log: " + logFile.getAbsolutePath());
      //     // Call your existing parser here
          
      // }
        // parser.parseLogSegment("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log" , logfile);

        InputStream inputStream = clientSocket.getInputStream();
        OutputStream outputStream = clientSocket.getOutputStream();

        byte[] mssgSize  = new byte[4];
        byte[] apiKey = new byte[2];
        byte[] apiVersion = new byte[2];
        byte[] correlationId = new byte[4];
        byte[] clientLenght , clientId ,remainingBytes ;
        parser.parseLogSegment("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log" , logfile);
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

        if(api == 75){
          clientLenght = new byte[2];
          inputStream.read(clientLenght);
          clientId = new byte[byteTool.byteArrayToInt(clientLenght)];
          inputStream.read(clientId);
          apiHandler.describePartitionHandler(inputStream, mssg ,responses , logfile);
          responseSize = byteArrayManipulation.sizeOfMessage(responses);
        }
        else if(api == 1){
          responseSize = byteArrayManipulation.sizeOfMessage(responses);
          apiHandler.fetchRequestHandler(logfile,responses,inputStream);
        }
        else{
          apiHandler.apiVersionsHandler(inputStream, mssg,version ,responses);
          responseSize = byteArrayManipulation.sizeOfMessage(responses);
          remainingBytes = new byte[mssg - 8];
          inputStream.read(remainingBytes);
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