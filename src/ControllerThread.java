import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;

public class ControllerThread implements Runnable {

    Socket controller;

    Socket dstoreIn;
    DStoreInfo info;
    PrintWriter out = null;
    BufferedReader in = null;

    String folderName;

    public ControllerThread(Socket controller, Socket dstoreIn, DStoreInfo infos,
        String folderName) {
        this.controller = controller;
        this.dstoreIn = dstoreIn;
        info = infos;
        this.folderName = folderName;
    }

    @Override
    public void run() {
        try {
            out = new PrintWriter(controller.getOutputStream(), true);
            in = new BufferedReader(
                new InputStreamReader(dstoreIn.getInputStream()));
            startThreadWaiters();
            handleCommand("LIST");

            String line;
            System.out.println("Controller thread " + controller.getPort() + " started");
            while ((line = in.readLine()) != null) {
                System.out.println("Controller thread " + line);
                handleCommand(line);
            }
            controller.close();
            System.out.println("ClientThread connection closed");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void handleCommand(String line) {
        System.out.println("***********Controller thread recieved " + line);
        if (line.startsWith("REMOVE")) {
            System.out.println("Controller thread recieved " + line);
            String[] input = line.split(" ");
            removeFile(input[1]);
        } else if (line.startsWith("REBALANCE")) {
            System.out.println("Controller thread recieved " + line);
            line=line.replaceAll("\\s+", " ");
            System.out.println("Controller thread recieved " + line);
            String[] input = line.split(" ");

            rebalance(input);

        } else if (line.startsWith("LIST")){
            System.out.println("Controller thread recieved " + line);
            System.out.println("Controller thread returned: LIST " + info.getFiles());
            out.println("LIST "+ info.getFiles());
        }
    }

    private void rebalance(String[] s) {
        if (s.length < 2) {
            System.out.println("Rebalance complete list too short");
            out.println("REBALANCE_COMPLETE");
            return;
        }
        int counter = 2;
        int addCounter = Integer.parseInt(s[1]);
        int addCurrent = 0;
        boolean inFile = false;
        boolean noCounted = false;
        int removeAmount = 0;
        int amount = 0;
        String message = "";
        while (counter < s.length) {
            if (addCurrent < addCounter) {

                message = "REBALANCE_STORE " + s[counter] + " " + info.getFileSize(s[counter]);

                amount = Integer.parseInt(s[counter + 1]);
                for (int i = counter + 2; i < counter + 2 + amount; i++) {
                    rebalanceMessage(Integer.parseInt(s[i]), message, s[counter]);
                    info.rebalanceWait();
                }
                counter = counter + 2 + amount;
                addCurrent++;
            } else {
                if (!noCounted) {
                    noCounted = true;
                    removeAmount = Integer.parseInt(s[counter]);
                    counter++;
                } else {
                    removeFile(s[counter]);
                    counter++;
                }

            }

        }
        System.out.println("Rebalance complete");
        System.out.println("CHECK");
        System.out.println("LIST OF FILES"+ info.getFiles());
        out.println("REBALANCE_COMPLETE");
    }

    private void rebalanceMessage(int port, String message, String fileName) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Socket socket = new Socket("localhost", port);
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(
                        new InputStreamReader(socket.getInputStream()));

                    out.println(message);
                    String line;
                    while ((line = in.readLine()) != null) {
                        System.out.println("Rebalance thread " + line);
                        if (line.startsWith("ACK")) {
                            OutputStream fileOut = socket.getOutputStream();
                            File folder = new File(System.getProperty("user.dir"), folderName);
                            File inputFile = new File(folder,fileName);
                            FileInputStream inf = new FileInputStream(inputFile);
                            byte[] buf = new byte[1024];
                            int buflen;
                            while ((buflen = inf.read(buf)) != -1) {
                                System.out.print("*");
                                fileOut.write(buf, 0, buflen);
                            }
                            out.close();
                            inf.close();
                            info.rebalanceNotify();
                        }

                    }


                    System.out.println("rebalance connection closed");

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (Exception e) {

                }
            }
        }, "Rebalance Thread").start();

    }

    private void removeFile(String s) {

        if (!info.checkFileExist(s)) {
            out.println("ERROR_FILE_DOES_NOT_EXIST " + s);
        }
        File folder = new File(System.getProperty("user.dir"), folderName);
        File file = new File(folder,s);
        if (file.delete()) {
            System.out.println("File deleted successfully");
            out.println("REMOVE_ACK " + s);
        } else {
            System.out.println("Failed to delete the file "+s);
        }
        info.removeFile(s);
    }

    private void startThreadWaiters() {
        new Thread(new Runnable() {
            @Override
            public void run() {

                while (info.storeFlag) {
                    System.out.println("STORE_ACK watcher started");
                    info.storeControllerMessage();
                    out.println("STORE_ACK " + info.storeMessage);
                    System.out.println("STORE_ACK " + info.storeMessage);
                }
            }
        }, "STORE_ACK Thread").start();
    }
}
