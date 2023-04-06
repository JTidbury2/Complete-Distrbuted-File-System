import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

public class DStoreThread implements Runnable {

  private Socket client;
  int port;
  ControllerInfo info;

  public DStoreThread(Socket client, int port, ControllerInfo info) {
    this.client = client;
    this.port = port;
    this.info = info;
  }

  private void handleCommand(String line) {

  }

  @Override
  public void run() {
    info.addDstore(port);
    BufferedReader in = null;
    try {
      in = new BufferedReader(
          new InputStreamReader(client.getInputStream()));
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
