import java.io.IOException;
import java.net.ServerSocket;

public class Controller {
  ServerSocket ss;
  public static void main (String[] args) {
    int cport = Integer.parseInt(args[0]);
    int repFactor = Integer.parseInt(args[1]);
    int timeOut = Integer.parseInt(args[2]);
    int rebalanceTime = Integer.parseInt(args[3]);

  }

  private void setUpCPort(int cport) {
    try {
      ss = new ServerSocket(cport);
    } catch (IOException e) {
      e.printStackTrace();
    }


  }

}
