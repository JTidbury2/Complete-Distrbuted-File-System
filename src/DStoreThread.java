import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Arrays;

public class DStoreThread implements Runnable {

    private Socket client;
    int port;
    ControllerInfo info;
    PrintWriter out = null;

    public DStoreThread(Socket client, int port, ControllerInfo infos) {
        this.client = client;
        this.port = port;
        info = infos;
    }

    private void handleCommand(String line) {
        if (line.startsWith("STORE_ACK")) {
            System.out.println("DStoreThread " + port + " recieved " + line);
            storeAckCommand(line);
        } else if (line.startsWith("REMOVE_ACK")) {
            System.out.println("DStoreThread " + port + " recieved " + line);
            removeAckCommand(line);
        } else if (line.startsWith("LIST")) {
            String[] input = line.split(" ");
            String[] files = Arrays.copyOfRange(input, 1, input.length);
            System.out.println("DStoreThread " + port + " recieved " + line);
            listCommand(files);
        }

    }

    private void listCommand(String[] files) {
        info.updateDstoreFiles(files, port);
    }


    private void storeAckCommand(String line) {
        String fileName = line.split(" ")[1];
        info.updateFileDstores(fileName, port);
        info.storeAck(fileName);
    }

    private void removeAckCommand(String line) {
        String fileName = line.split(" ")[1];
        info.removeAck(fileName);

    }

    @Override
    public void run() {
        BufferedReader in = null;
        try {
            in = new BufferedReader(
                new InputStreamReader(client.getInputStream()));
            out = new PrintWriter(new Socket("localhost", port).getOutputStream(), true);
            startThreadWaiters();
            out.println("LIST");

            String line;
            while ((line = in.readLine()) != null) {
                System.out.println("DStore " + port + "recieved: " + line);
                handleCommand(line);
            }
            client.close();
            System.out.println("DStoreThread" + port + "connection closed");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void startThreadWaiters() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (info.getRemoveFlag()) {
                    System.out.println("Remove watcher started");
                    info.removeWait();

                    out.println("REMOVE " + info.getRemoveFile());
                    System.out.println("REMOVE " + info.getRemoveFile());
                }
            }
        }, "Remove Start Watcher Thread").start();
    }
}
