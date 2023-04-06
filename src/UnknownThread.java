import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

public class UnknownThread implements Runnable{
  private Socket client;
  public UnknownThread(Socket client) {
    this.client = client;
  }


  @Override
  public void run() {
    setUpListener(client);
  }

  private void setUpListener(Socket client) {
    try {
      BufferedReader in = new BufferedReader( new InputStreamReader(client.getInputStream()));
      String line;
      while ((line = in.readLine()) != null) {
        handleCommand(line);
      }
      client.close();
    } catch (IOException e) {
      e.printStackTrace();
    }


  }

  private void handleCommand(String line) {
    //if (line.startsWith("JOIN"){


  }


}
