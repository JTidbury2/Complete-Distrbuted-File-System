import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class Controller {

  static ControllerInfo info = new ControllerInfo();
  static ServerSocket ss;
  static int cport = 0;
  static int repFactor = 0;
  static int timeOut = 0;
  static int rebalanceTime = 0;
  static ArrayList<String> dstoreList = new ArrayList<String>();

  public static void main(String[] args) {
    cport = Integer.parseInt(args[0]);
    repFactor = Integer.parseInt(args[1]);
    timeOut = Integer.parseInt(args[2]);
    rebalanceTime = Integer.parseInt(args[3]);
    info.setCport(cport);
    info.setRepFactor(repFactor);
    info.setTimeOut(timeOut);
    info.setRebalanceTime(rebalanceTime);
    System.out.println("ControllerInfo started");
    setUpCPort();
  }

  public static void setUpDstoreThread(Socket client, int port) {
    System.out.println("DStoreThread started");
    new Thread(new DStoreThread(client, port, info)).start();
  }

  public static void setUpClientThread(Socket client, String firstCommand) {
    System.out.println("ClientThread started");
    new Thread(new ClientThread(client, firstCommand, info)).start();
  }

  private static void setUpCPort() {
    try {
      ss = new ServerSocket(cport);
    } catch (IOException e) {
      e.printStackTrace();
    }
    while (true) {
      try {
        final Socket client = ss.accept();
        new Thread(new Runnable() {
          public void run() {
            try {
              boolean closeFlag = true;
              BufferedReader in = new BufferedReader(
                  new InputStreamReader(client.getInputStream()));
              String line;
              System.out.println("Connection to unknown accepted");
              while ((line = in.readLine()) != null) {
                if (line.startsWith("JOIN")) {
                  String[] split = line.split(" ");
                  String dstore = split[1];
                  dstoreList.add(dstore);
                  System.out.println("DStore " + dstore + " joined");
                  info.addDstore(Integer.parseInt(dstore));
                  setUpDstoreThread(client, Integer.parseInt(dstore));
                  closeFlag = false;
                  break;
                } else if ((line.startsWith("LIST") || line.startsWith("STORE") || line.startsWith(
                    "REMOVE"))) {
                  System.out.println("Command " + line + " received");
                  setUpClientThread(client, line);
                  closeFlag = false;
                  break;
                }
              }
              if (closeFlag) {
                client.close();
                System.out.println("Connection to unknown closed");
              }


            } catch (IOException e) {
            }
          }
        }).start();
      } catch (IOException e) {
        e.printStackTrace();
      }

    }


  }

}
