import java.util.ArrayList;
import java.util.HashMap;
/**
 * The DStoreInfo class encapsulates the information and behaviors related to a specific DStore.
 */
public class DStoreInfo {

    private final int port; // Port of the DStore
    private final int cport; // Port of the controller
    private final int timeOut; // Time-out period for the DStore

    /**
     * Constructor for the DStoreInfo class
     *
     * @param port     Port of the DStore
     * @param cport    Port of the controller
     * @param timeOut  Time-out period for the DStore
     */
    public DStoreInfo(int port, int cport, int timeOut) {
        this.port = port;
        this.cport = cport;
        this.timeOut = timeOut;
    }


    private final Object storeLock = new Object();
    private Object removeLock = new Object();
    boolean storeFlag = true;
    String storeMessage = null;

    private ArrayList<String> fileList = new ArrayList<>();

    Object rebalanceLock = new Object();

    private HashMap<String, Integer> fileSize = new HashMap<>();
    /**
     * This method makes the thread wait until the store message is ready.
     */
    public void storeControllerMessage() {
        synchronized (storeLock) {
            System.out.println("Store message wait");
            try {
                storeLock.wait();
                System.out.println("Store message wait done");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    /**
     * @return Time-out period for the DStore
     */
    public int getTimeOut() {
        return timeOut;
    }
    /**
     * This method retrieves the file size for a given file.
     *
     * @param fileName Name of the file
     * @return Size of the file
     */
    public int getFileSize(String fileName) {
        return fileSize.get(fileName);
    }
    /**
     * This method stores the size of a specific file in the fileSize map.
     *
     * @param fileName Name of the file
     * @param size Size of the file
     */
    public void addFileSize(String fileName, int size) {
        fileSize.put(fileName, size);
    }
    /**
     * This method wakes up all threads that are waiting on the storeLock object.
     * It also assigns the storeMessage variable the value of the passed in message.
     *
     * @param message Message that will be assigned to storeMessage
     */
    public void storeControllerMessageGo(String message) {
        synchronized (storeLock) {
            storeMessage = message;
            storeLock.notifyAll();
        }
    }
    /**
     * This method removes a specific file from the fileList ArrayList.
     *
     * @param s Name of the file to be removed
     */
    public void removeFile(String s) {
        fileList.remove(s);
    }
    /**
     * This method checks if a specific file exists in the fileList ArrayList.
     *
     * @param fileName Name of the file to be checked
     * @return True if the file exists, false otherwise
     */
    public boolean checkFileExist(String fileName) {
        return fileList.contains(fileName);
    }
    /**
     * This method adds a file to the fileList ArrayList if it is not already there.
     *
     * @param file Name of the file to be added
     */
    public void addFile(String file) {
        if (fileList.contains(file)) {
            return;
        }
        fileList.add(file);
    }

    /**
     * This method converts the file list into a single string.
     *
     * @return String representation of the file list
     */
    public String getFiles() {
        String files = "";
        for (String s : fileList) {
            files += s + " ";
        }
        files.trim();
        return files;
    }
    /**
     * This method puts the thread in wait state until the rebalance operation is finished.
     */
    public void rebalanceWait() {
        synchronized (rebalanceLock) {
            try {
                rebalanceLock.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    /**
     * This method notifies all waiting threads when the rebalance operation is finished.
     */
    public void rebalanceNotify() {
        synchronized (rebalanceLock) {
            rebalanceLock.notifyAll();
        }
    }
}
