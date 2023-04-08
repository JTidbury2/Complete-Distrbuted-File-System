import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.file.FileAlreadyExistsException;


public class ClientThread implements Runnable {

  Socket client;
  String firstCommand;
  ControllerInfo info;
  PrintWriter out = null;
  BufferedReader in = null;

  public ClientThread(Socket client, String firstCommand, ControllerInfo infos) {
    this.client = client;
    this.firstCommand = firstCommand;
    info = infos;
  }

  private void handleCommand(String line) {
    if (line.startsWith("LIST")) {
      listCommand();
    }else if (line.startsWith("STORE")){
      String[] input= line.split(" ");
      storeCommand(input[1],input[2]);
    }
  }

  private void storeCommand(String s, String s1) {
    info.setFileIndex(s,"SIP");
    String message = null;
    try {
      message = info.storeTo(s);
    } catch (NotEnoughDstoresException e) {
      out.println("ERROR_NOT_ENOUGH_DSTORES");
      e.printStackTrace();
    } catch (FileAlreadyExistsException e) {
      out.println("ERROR_FILE_ALREADY_EXISTS");
      e.printStackTrace();
    }
    out.println(message);
    System.out.println("Client thread returned "+message);
    info.storeWait();
    out.println("STORE_COMPLETE");

  }

  private void listCommand() {

    String message = null;
    try {
      message = info.list();
    } catch (NotEnoughDstoresException e) {
      message ="ERROR_NOT_ENOUGH_DSTORES";
    }
    out.println(message);
    System.out.println("Client thread returned"+message);
  }

  @Override
  public void run() {

    try {
      out = new PrintWriter(client.getOutputStream(), true);
      in = new BufferedReader(
          new InputStreamReader(client.getInputStream()));
          handleCommand(firstCommand);
      String line;
      System.out.println("ClientThread started 2");
      while ((line = in.readLine()) != null) {
        System.out.println("Client thread recieved"+line);
        handleCommand(line);
      }
      client.close();
      System.out.println("ClientThread connection closed");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
