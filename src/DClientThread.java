import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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

    public DClientThread(Socket client, String line, DStoreInfo infos) {
        this.client = client;
        this.firstCommand = line;
        info = infos;
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
            while ((line = in.readLine()) != null) {
                System.out.println("DClient thread " + client.getPort() + " received :" + line);
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
        }else if (line.startsWith("LOAD_DATA")) {
            String input = line.split(" ")[1];
            loadData(input);
        }
    }

    private void loadData(String fileName) {
        File outFile = new File(fileName);
        try {
            FileInputStream fileIn = new FileInputStream(outFile);
            byte[] content = new byte[(int) outFile.length()];
            int buflen ;
            while ((buflen = fileIn.read(content)) != -1){
                out.write(buflen);
            }

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }

    private void storeCommand(String filename, String filesize) {
        out.println("ACK");
        System.out.println("DClient thread " + client.getPort() + " ACK sent");

        int size = Integer.parseInt(filesize);
        byte[] content = new byte[size];
        try {
            inStream.readNBytes(content, 0, size);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("DClient thread " + client.getPort() + " File received");

        File outFile = new File(filename);
        try {
            outFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            FileOutputStream fileOut = new FileOutputStream(outFile);
            fileOut.write(content);
            fileOut.close();
            System.out.println("DClient thread " + client.getPort() + " File Created");
        } catch (IOException e) {
            e.printStackTrace();
        }
        // TODO add file folder funcitonality
        info.storeControllerMessageGo(filename);
    }
}
