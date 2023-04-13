import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ControllerThread implements Runnable {

    Socket controller;
    DStoreInfo info;
    PrintWriter out = null;
    BufferedReader in = null;

    public ControllerThread(Socket controller, DStoreInfo infos) {
        this.controller = controller;
        info = infos;
    }

    @Override
    public void run() {
        try {
            out = new PrintWriter(controller.getOutputStream(), true);
            in = new BufferedReader(
                new InputStreamReader(controller.getInputStream()));
            startThreadWaiters();

            String line;
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
        if (line.startsWith("REMOVE")) {
            String[] input = line.split(" ");
            removeFile(input[1]);
        }
    }

    private void removeFile(String s) {
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
