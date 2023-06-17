import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;
/**
 * Class representing a thread for a distributed store. It communicates
 * with the Controller and handles different types of commands.
 */
public class DStoreThread implements Runnable {
    boolean listWatcher = true;

    Timer rebalanceTimer = new Timer();

    boolean rebalanceTimout = false;

    private Socket client;
    int port;
    ControllerInfo info;
    PrintWriter out = null;
    /**
     * Constructor for DStoreThread.
     * @param client The client socket.
     * @param port The port number.
     * @param infos The information about the controller.
     */
    public DStoreThread(Socket client, int port, ControllerInfo infos) {
        this.client = client;
        this.port = port;
        info = infos;
    }
    /**
     * The run method is called when the thread starts. It reads commands
     * from the Controller and handles them accordingly.
     */
    @Override
    public void run() {
        BufferedReader in = null;
        String line = null;
        try {
            System.out.println("DStoreThread " + port + " started properly");
            in = new BufferedReader(
                new InputStreamReader(client.getInputStream()));
            out = new PrintWriter(client.getOutputStream(), true);
            info.setListWaitFlag(port,false);


            System.out.println("DStoreThread " + port + " started");
            boolean temp = startThreadWaiters();
            if (temp) {
                info.rebalanceStart();
            }

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
                    closeDstore();
                    System.out.println("DStore thread " + port + " closed client");
                } catch (IOException e1) {
                    System.out.println("client failed to close");
                    e1.printStackTrace();
                }
            }
            e.printStackTrace();
        }

    }
    /**
     * This method is used to remove the DStore from the Controller's list.
     */
    private void closeDstore() {
        info.removeDstore(port);
    }
    /**
     * Handles commands sent by the Controller.
     * @param line The command string.
     */
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
            listWatcher=true;
            listCommand(files);
        } else if (line.startsWith("REBALANCE_COMPLETE")) {
            System.out.println("DStoreThread " + port + " recieved " + line);
            if (!rebalanceTimout) {
                rebalanceTimer.cancel();
                info.rebalanceComplete();
            }


        }

    }
    /**
     * Handles the "STORE_ACK" command.
     * @param line The command string.
     */
    private void storeAckCommand(String line) {
        info.dstoreStoreAckCommmand(line, port);
    }
    /**
     * Handles the "REMOVE_ACK" command.
     * @param line The command string.
     */
    private void removeAckCommand(String line) {
        info.dstoreRemoveAckCommmand(line);
    }
    /**
     * Handles the "LIST" command.
     * @param files The list of files sent in the command.
     */
    private void listCommand(String[] files) {
        ArrayList<String> returnFiles =new ArrayList<>(Arrays.asList(files)) ;
        info.listRecieved(port, returnFiles);
    }
    /**
     * Starts threads that handle different conditions in the program.
     * Each thread waits for a certain condition to become true, then
     * performs a specific task.
     * @return Always returns true.
     */
    private boolean startThreadWaiters() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (info.getRemoveFlag()) {
                    System.out.println("Remove watcher started");

                    String temp = info.removeWait();
                    if (info.getDstorewithFile(temp).contains(port)){
                        out.println("REMOVE " + temp);
                        System.out.println("REMOVE " + temp);
                    }
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
                    rebalanceTimout = false;
                    message.replaceAll("\\s+", " ");
                    rebalanceTimer = new Timer();
                    out.println(message);
                    try {
                        rebalanceTimer.schedule(new TimerTask() {
                            @Override
                            public void run() {

                                info.rebalanceComplete();
                                rebalanceTimout = true;
                            }
                        }, info.getTimeOut());
                    } catch (IllegalStateException e){
                        System.out.println("Timer already cancelled");
                    }


                }
            }
        }, "Store Start Watcher Thread").start();


        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    System.out.println("List watcher started"  + port);

                    info.listWait(port);
                    if (listWatcher) {
                        out.println("LIST");
                        System.out.println("LIST sent" + port);
                    }

                }
            }
        }, "LIST Start Watcher Thread").start();

        return true;


    }

}
