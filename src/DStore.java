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
  static ServerSocket listenerSocket;
  static Socket clientSocket;
  static String[] fileList;
  public static void main (String[] args) {
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
      listenerSocket = new ServerSocket(port);
    } catch (IOException e) {
      e.printStackTrace();
    }
    try {
      clientSocket=listenerSocket.accept();
      try {
        BufferedReader in = new BufferedReader(
            new InputStreamReader(clientSocket.getInputStream()));
        String line;
        while ((line = in.readLine()) != null) {
          System.out.println(line + " received");
          //TODO: do something with the message
          handleCommand(line);
        }
        clientSocket.close();
      } catch (Exception e) {
      }
    } catch (IOException e) {
      e.printStackTrace();
    }


  }

  private static void handleCommand(String line) {
    if (line.startsWith("LIST")) {
      String message = "";
      for (String file : fileList) {
        message += file + " ";
      }
      message = message.trim();
      controllerOut.println(message);
    }
  }

  public static void connectToController() {
    try {
      controllerSocket = new Socket("localhost", cport);
      controllerOut = new PrintWriter(controllerSocket.getOutputStream());
      controllerOut.println("JOIN " + port);
      controllerOut.flush();
      controllerOut.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }




}
