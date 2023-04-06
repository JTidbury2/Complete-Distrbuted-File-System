import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

public class DStoreThread implements Runnable{
  private Socket client;
  int port;
  public DStoreThread(Socket client,int port) {
    this.client = client;
    this.port = port;
  }

  @Override
  public void run() {
    BufferedReader in = null;
    try {
      in = new BufferedReader(
          new InputStreamReader(client.getInputStream()));
      String line;
      System.out.println("DStoreThread started 2");
      while ((line = in.readLine()) != null) {
        System.out.println(line);
      }
      client.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
