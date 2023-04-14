import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Arrays;

public class DClientThread implements Runnable {

    Socket client;
    String firstCommand;

    OutputStream fileOut = null;
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
            fileOut = client.getOutputStream();
            inStream = client.getInputStream();
            System.out.println("DClient thread " + client.getPort() + " received :" + firstCommand);

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

    private void handleCommand(String line) throws IOException {
        if (line.startsWith("STORE")||line.startsWith("REBALANCE")) {
            String[] input = line.split(" ");
            storeCommand(input[1], input[2]);
        }else if (line.startsWith("LOAD_DATA")) {
            String input = line.split(" ")[1];
            loadData(input);
        }
    }

    private void loadData(String fileName) throws IOException {
        System.out.println("DClient thread " + client.getPort() + " LOAD_DATA "+ fileName+" received");
        if (!info.checkFileExist(fileName)) {
            System.out.println("DSTore file does not exist");
            client.close();
            return;
        }


        try {
            File inputFile = new File(fileName);
            FileInputStream inf = new FileInputStream(inputFile);
            byte[] buf = new byte[1024];
            int buflen;
            while ((buflen=inf.read(buf)) != -1){
                System.out.print("*");
                fileOut.write(buf,0,buflen);
            }
            inf.close();
            System.out.println("DClient thread " + client.getPort() + " File sent");

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
        info.addFile(filename);
        info.addFileSize(filename, size);
        info.storeControllerMessageGo(filename);
    }
}
