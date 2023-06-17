import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
/**
 * The main Dstore class represents a distributed file store that handles file operations.
 * It manages the connections to clients and controllers and initiates threads for handling their requests.
 */
public class Dstore {

    static int port = 0; // Dstore's port
    static int cport = 0; // Controller's port
    static int timeOut = 0; // Timeout for DStore
    static String fileFolder = ""; // Folder name to perform operations in
    static Socket controllerSocket; // Socket for controller connection
    static PrintWriter controllerOut; // Output stream for the controller socket
    static ServerSocket ss; // Server socket for accepting client connections
    static DStoreInfo info; // Object containing DStore related information

    /**
     * The main method for running the Dstore.
     *
     * @param args Array of arguments passed to the main method.
     *             It is expected to contain the Dstore's port, Controller's port, timeout and the file folder path.
     */
    public static void main(String[] args) {
        port = Integer.parseInt(args[0]);
        cport = Integer.parseInt(args[1]);
        timeOut = Integer.parseInt(args[2]);
        info=new DStoreInfo(port,cport,timeOut);
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
    /**
     * This method sets up the connection with the controller.
     */
    public static void connectToController() {
        try {
            controllerSocket = new Socket("localhost", cport);
            controllerOut = new PrintWriter(controllerSocket.getOutputStream(), true);
            controllerOut.println("JOIN " + port);
            setUpControllerThread(controllerSocket);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * This method sets up the listener port for accepting client connections.
     */
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
                                    setUpClientThread(client, line);
                                    closeFlag = false;
                                    System.out.println("Client thread started");
                                    break;

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
    /**
     * This method sets up a client thread for handling client requests.
     *
     * @param client Socket for client connection
     * @param line   First command from client
     */
    private static void setUpClientThread(Socket client, String line) {
        System.out.println("ClientThread " + client.getPort() + " started");
        new Thread(new DClientThread(client, line, info, fileFolder),
            "Client Thread " + client.getPort()).start();
    }
    /**
     * This method sets up a controller thread for handling controller requests.
     *
     * @param client Socket for controller connection
     */
    private static void setUpControllerThread(Socket client) {
        System.out.println("ControllerThread " + client.getPort() + " started");
        new Thread(new ControllerThread(client, info, fileFolder),
            "Controller Thread " + client.getPort()).start();
    }


}
