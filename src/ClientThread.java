import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;


public class ClientThread implements Runnable {

    Timer removeTimer = new Timer();

    boolean removeTimout = false;


    Socket client;
    String firstCommand;
    ControllerInfo info;
    PrintWriter out = null;
    BufferedReader in = null;

    boolean storeComplete = false;

    public ClientThread(Socket client, String firstCommand, ControllerInfo infos) {
        this.client = client;
        this.firstCommand = firstCommand;
        info = infos;
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

    private void handleCommand(String line) {
        info.checkRebalanceTakingPlace();
        if (line.startsWith("LIST")) {
            listCommand();
        } else if (line.startsWith("STORE")) {
            String[] input = line.split(" ");
            storeCommand(input[1], input[2]);
        } else if (line.startsWith("RELOAD") || line.startsWith("LOAD")) {
            if (line.startsWith("LOAD")) {
                info.setFileLoadTimes(line.split(" ")[1] + client.getPort(), 0);
            }
            String[] input = line.split(" ");
            loadCommand(input[1], info.getFileLoadTimes(input[1]+ client.getPort()) );
        } else if (line.startsWith("REMOVE")) {
            String[] input = line.split(" ");
            removeCommand(input[1]);
        }
    }

    private void listCommand() {
        System.out.println("List command started");
        String message = null;
        try {
            message = info.list();
        } catch (NotEnoughDstoresException e) {
            message = "ERROR_NOT_ENOUGH_DSTORES";
        }
        out.println(message);
        System.out.println("Client thread returned" + message);
    }

    private void storeCommand(String fileName, String fileSize) {
        System.out.println("Store command started");
        String message = null;
        try {
            message = info.clientStoreCommand(fileName, fileSize);
        } catch (NotEnoughDstoresException e) {
            message = "ERROR_NOT_ENOUGH_DSTORES";
            out.println(message);
            return;
        } catch (FileAlreadyExistsException e) {
            message = "ERROR_FILE_ALREADY_EXISTS";
            out.println(message);
            return;
        }
        out.println(message);
        System.out.println("Client thread " + client.getPort() + " returned " + message);
        storeComplete = true;
        Timer timer = new Timer();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                storeComplete = false;
                info.removeFileIndex(fileName);
                info.storeComplete();
                System.out.println("TIMEOUT ON STORE");

            }
        };

        timer.schedule(task, info.getTimeOut());

        info.storeWait();

        task.cancel();
        timer.cancel();

        //TODO add timeout features for recieinving all acks and sending store complete
        System.out.println("Store Complete = " + storeComplete);
        if (storeComplete) {
            out.println("STORE_COMPLETE");
            System.out.println("Client thread " + client.getPort() + " returned STORE_COMPLETE");
        }
    }


    private void loadCommand(String s, int times) {
        System.out.println("Load command started");
        try {
            System.out.println("Load command started" + s + " " + times);
            int[] fileInfo = info.getFileDStores(s, times);
            System.out.println("Load command started" + fileInfo[0] + " " + fileInfo[1] );
            int port = fileInfo[0];
            int filesize = fileInfo[1];
            String message = "LOAD_FROM " + port + " " + filesize;
            System.out.println(message);
            out.println(message);
        } catch (NotEnoughDstoresException e) {
            out.println("ERROR_NOT_ENOUGH_DSTORES");
        } catch (FileDoesNotExistException e) {
            out.println("ERROR_FILE_DOES_NOT_EXIST");
        } catch (DStoreCantRecieveException e) {
            out.println("ERROR_LOAD");
        }
    }

    private void removeCommand(String fileName) {
        removeTimout = false;
        System.out.println("Remove command started");
        try {
            info.clientRemoveCommand(fileName);
            removeTimer = new Timer();
            removeTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    setRemoveTimeout(true);
                    info.clearRemoveAcks(fileName);
                    info.removeAckStart();
                    System.out.println("TIMEOUT ON REMOVE");
                }
            }, info.getTimeOut());

        } catch (NotEnoughDstoresException e) {
            out.println("ERROR_NOT_ENOUGH_DSTORES");

        } catch (FileDoesNotExistException e) {
            out.println("ERROR_FILE_DOES_NOT_EXIST");
        }
    }

    private boolean getRemoveTimeout() {
        synchronized (this) {
            return removeTimout;
        }
    }

    private void setRemoveTimeout(boolean removeTimout) {
        synchronized (this) {
            this.removeTimout = removeTimout;
        }
    }


    private void startThreadWaiters() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (info.getRemoveAckFlag()) {
                    System.out.println("Remove ack watcher started");
                    info.removeAckWait();
                    removeTimer.cancel();
                    // TODO add timeout features for recieinving all acks and sending remove complete
                    if (!getRemoveTimeout()) {
                        out.println("REMOVE_COMPLETE");
                        System.out.println(
                            "Client thread " + client.getPort() + " returned REMOVE_COMPLETE");
                    } else {
                        System.out.println("TIMEOUT ON REMOVE");
                    }
                }
            }
        }, "Remove Ack Thread").start();
    }
}
