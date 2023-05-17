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
    String fileFolder;

    public DClientThread(Socket client, String line, DStoreInfo infos, String fileFolder) {
        this.client = client;
        this.firstCommand = line;
        info = infos;
        this.fileFolder = fileFolder;
    }

    @Override
    public void run() {
        try {
            client.setSoTimeout(info.getTimeOut());
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
        if (line.startsWith("STORE") ) {
            String[] input = line.split(" ");
            storeCommand(input[1], input[2],true);
        } else if (line.startsWith("REBALANCE")) {
            String[] input = line.split(" ");
            storeCommand(input[1], input[2],false);
        }else if (line.startsWith("LOAD_DATA")) {
            String input = line.split(" ")[1];
            loadData(input);
        }
    }

    private void storeCommand(String filename, String filesize, boolean isStore) {

        File folder = createFolder();

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

        File outFile = new File(folder, filename);
        if(outFile.exists()){
            System.out.println("DClient thread " + client.getPort() + " File already exists");
            return;
        }
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
        info.addFile(filename);
        info.addFileSize(filename, size);
        if (isStore) {
            info.storeControllerMessageGo(filename);
        }
    }

    private void loadData(String fileName) throws IOException {
        File folder = createFolder();
        System.out.println(
            "DClient thread " + client.getPort() + " LOAD_DATA " + fileName + " received");
        if (!info.checkFileExist(fileName)) {
            System.out.println("DSTore file does not exist");
            client.close();
            return;
        }

        try {

            File inputFile = new File(folder, fileName);
            FileInputStream inf = new FileInputStream(inputFile);
            byte[] buf = new byte[1024];
            int buflen;
            while ((buflen = inf.read(buf)) != -1) {
                System.out.print("*");
                fileOut.write(buf, 0, buflen);
            }
            inf.close();
            System.out.println("DClient thread " + client.getPort() + " File sent");

        } catch (FileNotFoundException e) {
            System.out.println("DSTore file does not exist");
            client.close();
        } catch (IOException e) {
            System.out.println("DSTore file does not exist");
            client.close();
        }


    }


    private File createFolder() {
        String folderName = fileFolder;
        File folder = new File(System.getProperty("user.dir"), folderName);

        if (!folder.exists()) {
            boolean created = folder.mkdirs();
            if (created) {
                System.out.println("Folder created successfully.");
            } else {
                System.out.println("Failed to create folder.");
            }
        } else {
            System.out.println("Folder already exists.");
        }
        return folder;
    }
}
