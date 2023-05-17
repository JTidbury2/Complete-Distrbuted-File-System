import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class ClientThread implements Runnable {

    Timer removeTimer = new Timer();



    boolean removeMe= false;


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
            handleCommand(firstCommand);

            String line;

            while ((line = in.readLine()) != null) {

                System.out.println("ClientThread" + client.getPort() + "received" + line);
                info.systemCheck(72);
                handleCommand(line);

            }
            client.close();
            info.systemCheck(70);
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
                info.setFileLoadTimes(line.split(" ")[1], client.getPort());
            }
            String[] input = line.split(" ");
            loadCommand(input[1]);
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
        info.systemCheck(69);
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


        CountDownLatch clientLatch = new CountDownLatch(info.getRepFactor());
        info.addStoreLatchMap(fileName, clientLatch);
        try {
            boolean completeStore = clientLatch.await(info.getTimeOut(), TimeUnit.MILLISECONDS);
            if (completeStore){
                info.storeFinished(fileName);
                out.println("STORE_COMPLETE");
                System.out.println("Client thread " + client.getPort() + " returned STORE_COMPLETE");
            } else {
                info.storeFailed(fileName);
                System.out.println("TIMEOUT ON STORE");
            }
            info.removeStoreLatchMap(fileName);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Store Complete = " + storeComplete);

    }


    private void loadCommand(String s) {
        System.out.println("Load command started");
        try {

            int[] fileInfo = info.getFileDStores(s, client.getPort());
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
        System.out.println("Remove command started");
        try {
            info.clientRemoveCommand(fileName);

            CountDownLatch removeLatch = new CountDownLatch(info.getRepFactor());
            info.addRemoveLatchMap(fileName, removeLatch);

            info.removeStart(fileName);
            boolean completeRemove = removeLatch.await(info.getTimeOut(), TimeUnit.MILLISECONDS);
            if (completeRemove){
                System.out.println("Remove complete");
                info.removeFileExistance(fileName);
                out.println("REMOVE_COMPLETE");
                System.out.println(
                    "Client thread " + client.getPort() + " returned REMOVE_COMPLETE");
            }else{
                info.removeFailed(fileName);
                System.out.println("TIMEOUT ON REMOVE");
            }

        } catch (NotEnoughDstoresException e) {
            out.println("ERROR_NOT_ENOUGH_DSTORES");

        } catch (FileDoesNotExistException e) {
            out.println("ERROR_FILE_DOES_NOT_EXIST");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }





}
