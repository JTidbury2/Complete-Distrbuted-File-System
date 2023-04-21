import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class DStore {

    static boolean closeTestFlag = true;

    static int port = 0;
    static int cport = 0;
    static int timeOut = 0;
    static String fileFolder = "";
    static Socket controllerSocket;
    static PrintWriter controllerOut;
    static ServerSocket ss;
    static Socket clientSocket;
    static String[] fileList;
    static DStoreInfo info = new DStoreInfo();

    public static void main(String[] args) {
        port = Integer.parseInt(args[0]);
        cport = Integer.parseInt(args[1]);
        timeOut = Integer.parseInt(args[2]);
        fileFolder = args[3];
        System.out.println("DStore " + port + " started");
        File folder = new File(System.getProperty("user.dir"), fileFolder);
        if (!folder.exists()) {
            folder.mkdir();
        }
        File[] flist = folder.listFiles();
        for (File file:flist){
            file.delete();
        }
        connectToController();
        System.out.println("DStore " + port + " connected to controller");
        setUpListenerPort();
    }

    public static void connectToController() {
        try {
            controllerSocket = new Socket("localhost", cport);
            controllerOut = new PrintWriter(controllerSocket.getOutputStream(), true);
            controllerOut.println("JOIN " + port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void setUpListenerPort() {
        try {
            ss = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
        }
        while (true) {
            try {
                final Socket client = ss.accept();
                new Thread(new Runnable() {
                    public void run() {
                        try {
                            boolean closeFlag = true;
                            BufferedReader in = new BufferedReader(
                                new InputStreamReader(client.getInputStream()));

                            String line;
                            System.out.println("Connection to unknown accepted");

                            while ((line = in.readLine()) != null) {
                                if (line.startsWith("LIST")) {
                                    setUpControllerThread(controllerSocket, client);
                                    System.out.println("Controller thread started");
                                    closeFlag = false;
                                    if (closeTestFlag) {
                                        closeTestFlag = false;
                                        System.out.println("Close test flag set to false");
                                        try {
                                            Thread.sleep(60000);

                                            controllerSocket.close();
                                            System.out.println("Controller thread ended");
                                        } catch (InterruptedException e) {
                                            throw new RuntimeException(e);
                                        }
                                    }

                                    break;
                                } else {
                                    setUpClientThread(client, line);
                                    closeFlag = false;
                                    System.out.println("Client thread started");
                                    break;
                                }
                            }
                            if (closeFlag) {
                                client.close();
                                System.out.println("Connection to unknown closed");
                            }
                        } catch (IOException e) {
                        }
                    }
                }).start();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }


    }

    private static void setUpClientThread(Socket client, String line) {
        System.out.println("ClientThread " + client.getPort() + " started");
        new Thread(new DClientThread(client, line, info, fileFolder),
            "Client Thread " + client.getPort()).start();
    }

    private static void setUpControllerThread(Socket client, Socket client2) {
        System.out.println("ControllerThread " + client.getPort() + " started");
        new Thread(new ControllerThread(client, client2, info, fileFolder),
            "Controller Thread " + client.getPort()).start();
    }


}
