import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

public class ClientThread implements Runnable{
  Socket client;
  String firstCommand;
  public ClientThread(Socket client,String firstCommand) {
    this.client = client;
    this.firstCommand = firstCommand;

  }
  private void handleCommand(String line) {
    //if (line.startsWith("JOIN"){
  }

  @Override
  public void run() {
    handleCommand(firstCommand);
    BufferedReader in = null;
    try {
      in = new BufferedReader(
          new InputStreamReader(client.getInputStream()));
      String line;
      System.out.println("ClientThread started 2");
      while ((line = in.readLine()) != null) {
        System.out.println(line);
      }
      client.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
