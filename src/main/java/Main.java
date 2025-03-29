
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
public class Main {
  public static void main(String[] args){

    System.err.println("Logs from your program will appear here!");
    int port = 9092;
    try (ServerSocket serverSocket = new ServerSocket(port)) {
      System.out.println("Server listening on port " + port);

      while (true) {
          // Accept a new client connection
          Socket clientSocket = serverSocket.accept();
          System.out.println("New client connected: " + clientSocket.getInetAddress());

          // Handle the client in a separate thread
          new ClientHandler(clientSocket).start();
      }
      } 
      catch (IOException e) {
        e.printStackTrace();
      }   
      
  }
}
