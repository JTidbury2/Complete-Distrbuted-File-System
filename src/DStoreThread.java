import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;

public class DStoreThread implements Runnable {

    Timer rebalanceTimer = new Timer();

    boolean rebalanceTimout = false;

    private Socket client;
    int port;
    ControllerInfo info;
    PrintWriter out = null;

    public DStoreThread(Socket client, int port, ControllerInfo infos) {
        this.client = client;
        this.port = port;
        info = infos;
    }

    @Override
    public void run() {
        BufferedReader in = null;
        String line = null;
        try {
            System.out.println("DStoreThread " + port + " started properly");
            in = new BufferedReader(
                new InputStreamReader(client.getInputStream()));
            out = new PrintWriter(client.getOutputStream(), true);
            startThreadWaiters();
            out.println("LIST");

            System.out.println("DStoreThread " + port + " started");

            while ((line = in.readLine()) != null) {
                System.out.println("DStore " + port + "recieved: " + line);
                handleCommand(line);
            }
            client.close();
            closeDstore();
            System.out.println("DStoreThread" + port + "connection closed");
        } catch (IOException e) {
            closeDstore();
            System.out.println("DStoreThread" + port + "connection closed2");
            System.out.println("DStoreThread" + port + " line = " + line);
            if (client != null && !client.isClosed()) {
                try {
                    client.close();
                    System.out.println("DStore thread " + port + " closed client");
                } catch (IOException e1) {
                    System.out.println("client failed to close");
                    e1.printStackTrace();
                }
            }
            e.printStackTrace();
        }

    }

    private void closeDstore() {
        info.removeDstore(port);
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
        } else if (line.startsWith("REBALANCE_COMPLETE")) {
            System.out.println("DStoreThread " + port + " recieved " + line);
            info.rebalanceComplete();
        }

    }

    private void storeAckCommand(String line) {
        info.dstoreStoreAckCommmand(line, port);
    }

    private void removeAckCommand(String line) {
        info.dstoreRemoveAckCommmand(line);
    }

    private void listCommand(String[] files) {
        info.rebalance(files, port);
    }

    private void startThreadWaiters() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (info.getRemoveFlag()) {
                    System.out.println("Remove watcher started");

                    String temp = info.removeWait();
                    out.println("REMOVE " + temp);
                    System.out.println("REMOVE " + temp);
                }
            }
        }, "Remove Start Watcher Thread").start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                while (info.isRebalanceFlag()) {
                    System.out.println("Rebalance watcher started");
                    info.rebalanceWait();
                    String files_to_remove = info.getRemoveFiles(port);
                    String files_to_send = info.getSendFiles(port);
                    files_to_send.trim();
                    files_to_remove.trim();
                    String message = "REBALANCE " + files_to_send + " " + files_to_remove;
                    message.replaceAll("\\s+", " ");
                    out.println(message);
                    rebalanceTimer.schedule(new TimerTask() {
                        @Override
                        public void run() {
                            info.rebalanceTimout();
                        }
                    }, info.getTimeOut());

                }
            }
        }, "Store Start Watcher Thread").start();


    }

}
