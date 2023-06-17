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
/**
 * This class represents a thread for handling DStore client related tasks.
 * It manages connections, handles commands, and performs file operations.
 */
public class DClientThread implements Runnable {

    Socket client; // Socket for client connection
    String firstCommand; // First command from client

    OutputStream fileOut = null; // Output stream for file operations
    PrintWriter out = null; // Output stream for the client socket
    BufferedReader in = null; // Input stream for the client socket
    InputStream inStream = null; // Input stream for file operations
    DStoreInfo info; // Object containing DStore related information
    String fileFolder; // Folder name to perform operations in
    /**
     * Initializes a new DClientThread object.
     *
     * @param client     Socket for client connection
     * @param line       First command from client
     * @param infos      DStoreInfo object
     * @param fileFolder Name of the folder
     */
    public DClientThread(Socket client, String line, DStoreInfo infos, String fileFolder) {
        this.client = client;
        this.firstCommand = line;
        info = infos;
        this.fileFolder = fileFolder;
    }
    /**
     * This is the entry point for the thread.
     * It performs DStore client operations until the connection closes.
     */
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
    /**
     * This method handles the received commands from the input stream.
     *
     * @param line String representing the command.
     * @throws IOException if there is an issue with the input or output stream
     */
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
    /**
     * This method handles store commands for storing a file.
     *
     * @param filename File name to be stored
     * @param filesize Size of the file to be stored
     * @param isStore  Flag to check if the file should be stored
     */
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
    /**
     * This method handles data load requests from a client.
     *
     * @param fileName File name to load data from
     * @throws IOException if there is an issue with the input or output stream
     */
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

    /**
     * This method creates a new folder for storing files.
     *
     * @return File object representing the created folder
     */
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
