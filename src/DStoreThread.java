import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class DStoreThread implements Runnable {

  private Socket client;
  int port;
  ControllerInfo info;
  PrintWriter out = null;

  public DStoreThread(Socket client, int port, ControllerInfo infos) {
    this.client = client;
    this.port = port;
    info = infos;
  }

  private void handleCommand(String line) {
    if (line.startsWith("STORE_ACK")) {
      System.out.println("DStoreThread recieved STORE_ACK");
      storeAckCommand(line);
    }

  }

  private void storeAckCommand(String line) {
    String fileName = line.split(" ")[1];
    info.storeAck(fileName);

  }

  @Override
  public void run() {
    BufferedReader in = null;
    try {
      in = new BufferedReader(
          new InputStreamReader(client.getInputStream()));
      out = new PrintWriter(new Socket("localhost",port).getOutputStream(), true);
      out.println("LIST");
      String line;
      System.out.println("DStoreThread started 2");
      while ((line = in.readLine()) != null) {
        System.out.println("DStore recieved"+line);
        handleCommand(line);
      }
      client.close();
      System.out.println("DStoreThread connection closed");
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
