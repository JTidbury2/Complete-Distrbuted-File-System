import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class DStore {

  static int port = 0;
  static int cport = 0;
  static int timeOut = 0;
  String fileFolder = "";
  static Socket controllerSocket;
  static PrintWriter controllerOut;
  static ServerSocket ss;
  static Socket clientSocket;
  static String[] fileList;
  static DStoreInfo info;
  public static void main(String[] args) {
    port = Integer.parseInt(args[0]);
    cport = Integer.parseInt(args[1]);
    timeOut = Integer.parseInt(args[2]);
    String fileFolder = args[3];
    System.out.println("DStore started");
    connectToController();
    System.out.println("DStore connected to controller");
    setUpListenerPort();
  }

  private static void setUpListenerPort() {
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
                if (line.startsWith("LIST")) {
                  setUpControllerThread(client);
                  closeFlag = false;
                  break;
                } else{
                  System.out.println("Command " + line + " received");
                  setUpClientThread(client, line);
                  closeFlag = false;
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

  private static void setUpClientThread(Socket client, String line) {
    System.out.println("ClientThread started");
    new Thread(new DClientThread(client, line,info)).start();
  }

  private static void setUpControllerThread(Socket client) {
    System.out.println("ControllerThread started");
    new Thread(new ControllerThread(client,info)).start();
  }

  private static void handleCommand(String line) {
    if (line.startsWith("LIST")) {
      String message = "";
      for (String file : fileList) {
        message += file + " ";
      }
      message = message.trim();
      controllerOut.println(message);
    } else if (line.startsWith("STORE")) {

    }

  }

  public static void connectToController() {
    try {
      controllerSocket = new Socket("localhost", cport);
      controllerOut = new PrintWriter(controllerSocket.getOutputStream(),true);
      controllerOut.println("JOIN " + port);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }


}
