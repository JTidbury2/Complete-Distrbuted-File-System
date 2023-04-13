import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ControllerThread implements Runnable {

    Socket controller;

    Socket dstoreIn;
    DStoreInfo info;
    PrintWriter out = null;
    BufferedReader in = null;

    public ControllerThread(Socket controller,Socket dstoreIn, DStoreInfo infos) {
        this.controller = controller;
        this.dstoreIn = dstoreIn;
        info = infos;
    }

    @Override
    public void run() {
        try {
            out = new PrintWriter(controller.getOutputStream(), true);
            in = new BufferedReader(
                new InputStreamReader(dstoreIn.getInputStream()));
            startThreadWaiters();

            String line;
            System.out.println("Controller thread " + controller.getPort()+" started");
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
        }
    }

    private void removeFile(String s) {
        if (!info.checkFileExist(s)){
            out.println("ERROR_FILE_DOES_NOT_EXIST " + s);
        }
        File file = new File(s);
        if (file.delete()) {
            System.out.println("File deleted successfully");
            out.println("REMOVE_ACK " + s);
        } else {
            System.out.println("Failed to delete the file");
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
        },"STORE_ACK Thread").start();
    }
}
