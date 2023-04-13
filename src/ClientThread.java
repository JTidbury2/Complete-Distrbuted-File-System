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
        } else if (line.startsWith("STORE")) {
            String[] input = line.split(" ");
            storeCommand(input[1], input[2]);
        } else if (line.startsWith("LOAD")) {
            String[] input = line.split(" ");
            loadCommand(input[1], 1);
        } else if (line.startsWith("REMOVE")) {
            String[] input = line.split(" ");
            removeCommand(input[1]);
        }
    }

    private void removeCommand(String fileName) {
        info.setFileIndex(fileName, Index.REMOVE_IN_PROGRESS);
        System.out.println("Client thread " + client.getPort() + " received REMOVE " + fileName);
        info.removeStart(fileName);
    }


    private void storeCommand(String s, String s1) {
        info.setFileIndex(s, Index.STORE_IN_PROGRESS);
        info.updateFileSize(s, Integer.parseInt(s1));
        String message = null;
        try {
            message = info.storeTo(s);
        } catch (NotEnoughDstoresException e) {
            message = "ERROR_NOT_ENOUGH_DSTORES";
        } catch (FileAlreadyExistsException e) {
            message = "ERROR_FILE_ALREADY_EXISTS";
        }
        out.println(message);
        System.out.println("Client thread " + client.getPort() + " returned " + message);
        info.storeWait();
        out.println("STORE_COMPLETE");
        System.out.println("Client thread " + client.getPort() + " returned STORE_COMPLETE");
    }

    private void loadCommand(String s, int times) {
        try {
            int[] fileInfo = info.getFileDStores(s, times);
            int port = fileInfo[0];
            int filesize = fileInfo[1];
            String message = "LOAD_FROM " + port + " " + filesize;
            out.println(message);
        } catch (NotEnoughDstoresException e) {
            throw new RuntimeException(e);
        } catch (FileDoesNotExistException e) {
            throw new RuntimeException(e);
        } catch (DStoreCantRecieveException e) {
            throw new RuntimeException(e);
        }
    }

    private void listCommand() {
        String message = null;
        try {
            message = info.list();
        } catch (NotEnoughDstoresException e) {
            message = "ERROR_NOT_ENOUGH_DSTORES";
        }
        out.println(message);
        System.out.println("Client thread returned" + message);
    }

    @Override
    public void run() {

        try {
            out = new PrintWriter(client.getOutputStream(), true);
            in = new BufferedReader(
                new InputStreamReader(client.getInputStream()));
            startThreadWaiters();
            handleCommand(firstCommand);

            String line;

            while ((line = in.readLine()) != null) {
                System.out.println("ClientThread" + client.getPort() + "received" + line);
                handleCommand(line);
            }
            client.close();
            System.out.println("ClientThread " + client.getPort() + " connection closed");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void startThreadWaiters() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (info.getRemoveAckFlag()) {
                    System.out.println("Remove ack watcher started");
                    info.removeAckWait();
                    out.println("REMOVE_COMPLETE");
                    System.out.println("REMOVE_COMPLETE");
                }
            }
        }, "Remove Ack Thread").start();
    }
}
