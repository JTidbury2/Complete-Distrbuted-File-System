import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ControllerThread implements Runnable{
Socket controller;
DStoreInfo info;
PrintWriter out = null;
BufferedReader in = null;
  public ControllerThread(Socket controller, DStoreInfo infos) {
  this.controller = controller;
  info = infos;
  }

  @Override
  public void run() {
    try {
      out = new PrintWriter(controller.getOutputStream(), true);
      in = new BufferedReader(
          new InputStreamReader(controller.getInputStream()));
      String line;
      new Thread(new Runnable() {
        @Override
        public void run() {
            info.storeControllerMessage();
            System.out.println("STORE_ACK " + info.storeMessage);
            out.println("STORE_ACK " + info.storeMessage);
        }
      }).start();
      System.out.println("ClientThread started 2");
      while ((line = in.readLine()) != null) {
        System.out.println("Client thread recieved" + line);
        handleCommand(line);
      }
      controller.close();
      System.out.println("ClientThread connection closed");
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  private void handleCommand(String line) {
  }
}
