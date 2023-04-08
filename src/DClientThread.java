import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class DClientThread implements Runnable {

  Socket client;
  String firstCommand;
  PrintWriter out = null;
  BufferedReader in = null;
  InputStream inStream = null;
  DStoreInfo info;

  public DClientThread(Socket client, String line, DStoreInfo info) {
    this.client = client;
    this.firstCommand = line;
    this.info = info;
  }

  @Override
  public void run() {
    try {
      out = new PrintWriter(client.getOutputStream(), true);
      in = new BufferedReader(
          new InputStreamReader(client.getInputStream()));
      inStream = client.getInputStream();
      handleCommand(firstCommand);
      String line;
      System.out.println("ClientThread started 2");
      while ((line = in.readLine()) != null) {
        System.out.println("Client thread recieved" + line);
        handleCommand(line);
      }
      client.close();
      System.out.println("ClientThread connection closed");
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  private void handleCommand(String line) {
    if (line.startsWith("STORE")) {
      String[] input = line.split(" ");
      storeCommand(input[1], input[2]);
    }
  }

  private void storeCommand(String filename, String filesize) {
    int size = Integer.parseInt(filesize);
    byte[] content = new byte[size];
    try {
      inStream.readNBytes(content, 0, size);
    } catch (IOException e) {
      e.printStackTrace();
    }
    System.out.println("File recieved");
    File outFile = new File(filename);
    try {
      outFile.createNewFile();
    } catch (IOException e) {
      e.printStackTrace();
    }
    System.out.println("File created");
    try {
      FileOutputStream fileOut = new FileOutputStream(outFile);
      fileOut.write(content);
      fileOut.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    // TODO add file folder funcitonality
    info.storeControllerMessageGo(filename);




  }
}
