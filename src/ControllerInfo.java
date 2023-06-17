import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
/**
 * This class serves as a controller for managing data stores (dStores) in a distributed system.
 * It includes functionalities such as rebalancing, maintaining stores' metadata, and handling asynchronous operations.
 *
 * @author JamesTidbury
 * @version 2.0
 */

public class ControllerInfo {
    // Timer to schedule rebalancing operations
    Timer timer = new Timer();

    // List of currently active data stores
    ArrayList<Integer> occuringDstoreList = new ArrayList<Integer>();
    /**
     * Adds a data store to the list of occurring data stores
     *
     * @param port The port number of the data store to be added
     */
    public void putOccuringDstoreList(int port){
        occuringDstoreList.add(port);
    }
    /**
     * Removes a data store from the list of occurring data stores
     *
     * @param port The port number of the data store to be removed
     */
    public void removeOccuringDstoreList(int port){
        occuringDstoreList.remove(port);
    }
    /**
     * Returns the size of the list of occurring data stores
     *
     * @return The size of the list of occurring data stores
     */
    public int getOccuringDstoreListSize(){
        return occuringDstoreList.size();
    }
    /**
     * Constructor to initialize the ControllerInfo with port, replication factor, timeout, and rebalance time.
     *
     * @param cport The port number of the controller
     * @param repFactor The replication factor of the data
     * @param timeOut The time out value for operations
     * @param rebalanceTime The time interval for rebalance operation
     */
    public ControllerInfo(int cport, int repFactor, int timeOut, int rebalanceTime) {
        this.cport = cport;
        this.repFactor = repFactor;
        this.timeOut = timeOut;
        this.rebalanceTime = rebalanceTime;

// Schedule a rebalance operation with given rebalance time
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                System.out.println("Rebalance timer started");
                rebalanceStart();

            }
        },getRebalanceTime(),getRebalanceTime());
    }

    HashMap<Integer, Boolean> listWaitFlag = new HashMap<Integer, Boolean>();

    ArrayList<Integer> rebalanceDstoreList = new ArrayList<Integer>();

    boolean rebalanceFinished = true;

    private int cport = 0;
    private int repFactor = 0;
    private int timeOut = 0;
    private int rebalanceTime = 0;

    private boolean listFlag = true;

    private HashMap<String, CountDownLatch> storeLatchMap = new HashMap<String, CountDownLatch>();

    private HashMap<Integer, ArrayList<String>> listReturnMap = new HashMap<Integer, ArrayList<String>>();

    private HashMap<String, CountDownLatch> removeLatchMap = new HashMap<String, CountDownLatch>();
    private ArrayList<Integer> dstoreList = new ArrayList<Integer>();
    private ArrayList<String> fileList = new ArrayList<String>();
    private HashMap<String, ArrayList<Integer>> fileDstoreMap = new HashMap<String, ArrayList<Integer>>();

    private HashMap<String, ArrayList<Integer>> newFileDstoreMap = new HashMap<String,
        ArrayList<Integer>>();

    private HashMap<Integer, ArrayList<String>> dstoreFileMap = new HashMap<Integer, ArrayList<String>>();

    private HashMap<Integer, ArrayList<String>> newDstoreFileMap = new HashMap<Integer,
        ArrayList<String>>();

    private HashMap<Integer, ArrayList<String>> new2DstoreFileMap = new HashMap<Integer,
        ArrayList<String>>();


    private HashMap<Integer, ArrayList<String>> dstoreRemoveMap = new HashMap<Integer, ArrayList<String>>();

    HashMap<String, ArrayList<Integer>> dstoreNeedMap = new HashMap<>();

    private HashMap<String, Index> fileIndex = new HashMap<String, Index>();
    private HashMap<String, Integer> fileSizeMap = new HashMap<String, Integer>();


    private ArrayList<Integer> reloadAck = new ArrayList<>();

    private HashMap<String, ArrayList<Integer>> fileLoadRecord = new HashMap<String,
        ArrayList<Integer>>();

    private boolean rebalanveTakingPlace = false;

    private Object isRebalanceLock = new Object();
    private Object storeLock = new Object();

    private Object joinLock = new Object();

    private Object removeLock = new Object();

    private Object rebalanceLock = new Object();

    private String removeFile = null;

    private String addFile = null;

    int listWaitSize = 0;

    boolean removeFlag = true;
    boolean rebalanceFlag = true;

    CountDownLatch rebalanceLatch;

    Object fileLock = new Object();
    /**
     * Sets the flag that indicates if the list at given port is waiting
     *
     * @param port The port number of the data store
     * @param flag The flag to set
     */
    public void setListWaitFlag(int port, boolean flag) {
        synchronized (fileLock) {
            listWaitFlag.put(port, flag);
        }
    }
    /**
     * Returns whether all lists at data stores are not waiting.
     *
     * @return true if no list is waiting, false otherwise
     */
    public boolean getListWaitFlag() {
        synchronized (fileLock) {
            System.out.println("listWaitFlag is " + listWaitFlag);
            listWaitSize = listWaitFlag.size();
            boolean temp = listWaitFlag.containsValue(false);
            return (!temp);
        }
    }

    /**
     * Adds a CountDownLatch associated with a file to the removeLatchMap.
     *
     * @param file The name of the file
     * @param latch The CountDownLatch associated with the file
     */
    public void addRemoveLatchMap(String file, CountDownLatch latch) {
        synchronized (fileLock) {
            removeLatchMap.put(file, latch);
        }
    }

    /**
     * Adds a CountDownLatch associated with a file to the storeLatchMap.
     *
     * @param file The name of the file
     * @param latch The CountDownLatch associated with the file
     */
    public void addStoreLatchMap(String file, CountDownLatch latch) {
        synchronized (fileLock) {
            storeLatchMap.put(file, latch);
        }
    }
    /**
     * Removes a CountDownLatch associated with a file from the storeLatchMap.
     *
     * @param file The name of the file
     */
    public void removeStoreLatchMap(String file) {
        synchronized (fileLock) {
            storeLatchMap.remove(file);
        }
    }
    /**
     * Removes a data store from the list of data stores.
     *
     * @param port The port number of the data store to be removed
     */
    public void removeDstoreList(int port) {
        synchronized (fileLock) {
            dstoreList.remove((Integer) port);
        }
    }

    /**
     * Checks if the rebalance flag is set.
     *
     * @return The state of the rebalance flag
     */
    public boolean isRebalanceFlag() {
        synchronized (fileLock) {
            return rebalanceFlag;
        }
    }

    /**
     * Checks if a rebalance operation is taking place.
     *
     * @return true if a rebalance operation is taking place, false otherwise
     */
    public boolean getRebalanveTakingPlace() {
        synchronized (fileLock) {
            return rebalanveTakingPlace;
        }

    }
    /**
     * Sets the flag that indicates if a rebalance operation is taking place.
     *
     * @param flag The flag to set
     */
    public void setRebalanveTakingPlace(boolean flag) {
        synchronized (fileLock) {
            rebalanveTakingPlace = flag;
        }
    }
    /**
     * Makes the current thread wait until a rebalance operation is done.
     */
    public void isRebalanceWait() {
        synchronized (isRebalanceLock) {
            try {
                isRebalanceLock.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
    /**
     * Wakes up all threads that are waiting on this object's monitor.
     */
    public void isRebalanceNotify() {
        synchronized (isRebalanceLock) {
            isRebalanceLock.notifyAll();
            System.out.println("All notified");
        }
    }
    /**
     * Removes a file's index from the index map.
     *
     * @param file The name of the file to remove its index
     */
    public void removeFileIndex(String file) {
        synchronized (fileLock) {
            fileIndex.remove(file);

        }
    }
    /**
     * Handles a failed store operation. It removes the file's index and
     * the association of the file with data stores, and removes the first
     * data store from the list of currently operating data stores.
     *
     * @param file The name of the file for which the store operation failed
     */
    public void storeFailed(String file) {
        synchronized (fileLock) {
            removeFileIndex(file);
            removeFileDstoreMap(file);
            removeDstoreFilemap(file);
            removeOccuringDstoreList(0);
        }
    }
    /**
     * Starts the rebalance operation in the system.
     * The rebalance operation attempts to evenly distribute data across all available data stores.
     * If a rebalance operation is already in progress or there are not enough data stores,
     * it will simply return and print a corresponding message.
     * After some checks, it starts the list operation and waits for it to finish,
     * then updates the relevant maps, checks the system,
     * and finally starts the actual rebalance if needed.
     */
    public void rebalanceStart() {


        synchronized (fileLock) {
            if (getRebalanveTakingPlace()) {
                System.out.println("Rebalance is taking place");
                return;
            }
            if ((dstoreList.size() < repFactor)) {
                System.out.println("Not enough dstores to rebalance");
                isRebalanceNotify();
                return;
            }
            setRebalanveTakingPlace(true);
            timer.cancel();
        }
        System.out.println("Rebalance start");
        System.out.println("listwaitflag is " + getListWaitFlag());

        while ((!checkIndex()) || (!getListWaitFlag())) {
            try {
                Thread.sleep(10);
                System.out.println("Waiting for index to complete");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        rebalanceLatch = new CountDownLatch(listWaitSize);
        System.out.println("Rebalance latch count is " + rebalanceLatch.getCount());
        rebalanceFinished = true;
        listReturnMap.clear();
        listStart();

        try {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {

                        rebalanceFinished = rebalanceLatch.await(getTimeOut(),
                            TimeUnit.MILLISECONDS);
                        System.out.println("REBALANCE TIMEOUT" + rebalanceFinished);
                        synchronized (fileLock) {
                            if (listReturnMap.size() < repFactor) {
                                setRebalanveTakingPlace(false);
                                timer= new Timer();
                                timer.scheduleAtFixedRate(new TimerTask() {
                                    @Override
                                    public void run() {
                                        System.out.println("Rebalance timer started");
                                        rebalanceStart();

                                    }
                                },getRebalanceTime(),getRebalanceTime());
                                return;
                            }



                            updateRelevantMaps();
                            systemCheck(0);
                            if(checkIfRebalanceNotNeeded()){
                                System.out.println("Rebalance not needed");
                                setRebalanveTakingPlace(false);
                                timer= new Timer();
                                timer.scheduleAtFixedRate(new TimerTask() {
                                    @Override
                                    public void run() {
                                        System.out.println("Rebalance timer started");
                                        rebalanceStart();

                                    }
                                },getRebalanceTime(),getRebalanceTime());
                                return;
                            }
                            rebalance();


                        }

                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                }
            }).start();
        } catch (Exception e) {

        }
    }
    /**
     * Checks if a rebalance operation is not needed by assessing if each file is
     * stored at the replication factor number of data stores, and if each data store
     * contains the correct amount of files according to the replication factor.
     *
     * @return true if rebalance is not needed, false otherwise
     */
    private boolean checkIfRebalanceNotNeeded(){
        synchronized(fileLock){
            for(String file:fileList){
                if(fileDstoreMap.get(file).size()<repFactor){
                    System.out.println("File "+file+" has less than repFactor dstores");
                    return false;
                }
            }
            for (int port: dstoreList){
                if(dstoreFileMap.containsKey(port) && dstoreFileMap.get(port).size()>Math.ceil((double) (repFactor * fileList.size()) /dstoreList.size())){
                    System.out.println("Dstore "+port+" has more than repFactor files");
                    System.out.println("Dstore "+port+" has "+dstoreFileMap.get(port).size()+" files");
                    System.out.println("Repfactor is "+repFactor);
                    System.out.println("fileList.size() is "+fileList.size());
                    System.out.println("dstoreList.size() is "+dstoreList.size());
                    System.out.println("repFactor*fileList.size())/dstoreList.size()="+((double) (repFactor * fileList.size()) /dstoreList.size()));
                    System.out.println("Math.ceil((repFactor*fileList.size())/dstoreList.size()) is "+Math.ceil((repFactor*fileList.size())/dstoreList.size()));
                    return false;
                }
                if(dstoreFileMap.containsKey(port) && dstoreFileMap.get(port).size()<Math.floor((repFactor*fileList.size())/dstoreList.size())){
                    System.out.println("Dstore "+port+" has less than repFactor files");
                    return false;
                }
            }
            return true;
        }
    }
    /**
     * Updates the file list and the maps that keep track of which files are stored
     * at which data stores, and which files each data store has.
     */
    private void updateRelevantMaps() {
        synchronized (fileLock) {
            System.out.println("UPDATING RELEVANT MAPS");
            fileList.clear();
            fileDstoreMap.clear();
            dstoreFileMap.clear();
            rebalanceDstoreList.clear();
            System.out.println("listReturnMap is " + listReturnMap);
            for (int port : listReturnMap.keySet()) {
                rebalanceDstoreList.add(port);
                if (listReturnMap.get(port).size() == 0) {
                    dstoreFileMap.put(port, new ArrayList<String>());
                }
                for (String file : listReturnMap.get(port)) {
                    //Update file list
                    if ((!fileList.contains(file))
                        && fileIndex.containsKey(file) &&fileIndex.get(file) == Index.STORE_COMPLETE) {
                        fileList.add(file);
                    }
                    //Update file dstore map
                    if (fileDstoreMap.containsKey(file)) {
                        fileDstoreMap.get(file).add(port);
                    } else {
                        ArrayList<Integer> dstores = new ArrayList<Integer>();
                        dstores.add(port);
                        fileDstoreMap.put(file, dstores);
                    }
                    //Update dstore file map
                    if (dstoreFileMap.containsKey(port)) {
                        dstoreFileMap.get(port).add(file);
                    } else {
                        ArrayList<String> files = new ArrayList<String>();
                        files.add(file);
                        dstoreFileMap.put(port, files);
                    }

                }
            }
            fileIndex.clear();
            for (String file : fileList) {
                fileIndex.put(file, Index.STORE_COMPLETE);
            }
        }

    }
    /**
     * Processes the list of files received from a data store.
     *
     * @param port The port number of the data store that sent the list
     * @param files The list of files received
     */
    public void listRecieved(int port, ArrayList<String> files) {
        synchronized (fileLock) {
            if (rebalanceFinished) {
                listReturnMap.put(port, files);
                rebalanceLatch.countDown();
            }

        }
    }
    /**
     * Initiates a rebalance operation across all data stores.
     */

    public void rebalance() {
        synchronized (fileLock) {
            synchronized (rebalanceLock) {
                System.out.println("REBALANCE STARTED");
                rebalanceDStores();
                rebalanceLock.notifyAll();
            }
        }
    }
    /**
     * Checks if there are no data stores currently being updated.
     *
     * @return true if no data store is being updated, false otherwise
     */
    private boolean checkIndex() {
        synchronized (fileLock) {
            return getOccuringDstoreListSize()==0;
        }
    }

    /**
     * Checks if a file exists in the file list and if there are enough data stores
     * available for storing or retrieving the file.
     *
     * @param fileName The name of the file
     * @return true if the file exists and there are enough data stores, false otherwise
     * @throws NotEnoughDstoresException if there are not enough data stores
     */
    public boolean checkFile(String fileName) throws NotEnoughDstoresException {
        synchronized (fileLock) {
            if (dstoreList.size() < repFactor) {
                throw new NotEnoughDstoresException();
            }
            return fileList.contains(fileName);
        }
    }

    /**
     * Returns the remove flag state.
     *
     * @return The state of the remove flag
     */
    public boolean getRemoveFlag() {
        synchronized (fileLock) {
            return removeFlag;
        }
    }
    /**
     * Updates the maps that keep track of which data stores a file is stored at, and
     * which files a data store has.
     *
     * @param fileName The name of the file
     * @param dstore The data store ID
     */
    public void updateFileDstores(String fileName, Integer dstore) {
        synchronized (fileLock) {
            if (fileDstoreMap.containsKey(fileName) && !fileDstoreMap.get(fileName)
                .contains(dstore)) {
                fileDstoreMap.get(fileName).add(dstore);
            } else if (!fileDstoreMap.containsKey(fileName)) {
                ArrayList<Integer> dstores = new ArrayList<Integer>();
                dstores.add(dstore);
                fileDstoreMap.put(fileName, dstores);
            }
            if (dstoreFileMap.containsKey(dstore) && !dstoreFileMap.get(dstore)
                .contains(fileName)) {
                dstoreFileMap.get(dstore).add(fileName);
            } else if (!dstoreFileMap.containsKey(dstore)) {
                ArrayList<String> files = new ArrayList<String>();
                files.add(fileName);
                dstoreFileMap.put(dstore, files);
            }
        }
    }

    /**
     * Updates the size of a file in the size map.
     *
     * @param fileName The name of the file
     * @param size The size of the file
     */
    public void updateFileSize(String fileName, Integer size) {
        synchronized (fileLock) {
            fileSizeMap.put(fileName, size);
        }
    }

    /**
     * Selects a set of data stores for storing a file.
     *
     * @return An array containing the IDs of the selected data stores
     */
    public Integer[] getStoreDStores() {
        synchronized (fileLock) {
            ArrayList<Integer> currentDstores = new ArrayList<>();
            ArrayList<Integer> allDstores = new ArrayList<>(dstoreList);
            for (int k = 0; k < repFactor; k++) {
                Integer minDstore = getMinStoreDstore(allDstores);
                currentDstores.add(minDstore);
                allDstores.remove(minDstore);
            }
            return currentDstores.toArray(new Integer[0]);
        }
    }
    /**
     * Selects the data store with the smallest number of stored files from a given list.
     *
     * @param dstoreList The list of data store IDs to choose from
     * @return The ID of the selected data store
     */
    public Integer getMinStoreDstore(ArrayList<Integer> dstoreList) {
        synchronized (fileLock) {
            //TODO start again stores
            Integer min = dstoreFileMap.getOrDefault(dstoreList.get(0),
                new ArrayList<>()).size();
            Integer minDstore = dstoreList.get(0);
            for (int dstore : dstoreList) {
                if (dstoreFileMap.getOrDefault(dstore, new ArrayList<>()).size()
                    < min) {
                    min = dstoreFileMap.getOrDefault(dstore, new ArrayList<>()).size();
                    minDstore = dstore;
                }
            }
            return minDstore;
        }
    }


    /**
     * Returns a string message listing all the files currently stored.
     *
     * @return A string message listing all the files
     * @throws NotEnoughDstoresException if there are not enough data stores
     */
    public String list() throws NotEnoughDstoresException {
        synchronized (fileLock) {
            if (dstoreList.size() < repFactor) {
                throw new NotEnoughDstoresException();
            }
            String message = "";
            for (String file : fileList) {
                message += file + " ";

            }
            message.trim();
            message = "LIST " + message;
            return message;
        }
    }
    /**
     * This method is used to store a file with the given name in the datastore.
     * It synchronizes on the file lock, checks that there are enough datastores,
     * and checks if the file already exists.
     *
     * @param name The name of the file to be stored.
     * @return A string that indicates which datastores the file should be stored in.
     * @throws NotEnoughDstoresException if there are not enough datastores to store the file.
     * @throws FileAlreadyExistsException if a file with the given name already exists.
     */
    public String storeTo(String name)
        throws NotEnoughDstoresException, FileAlreadyExistsException {
        synchronized (fileLock) {
            if (dstoreList.size() < repFactor) {
                throw new NotEnoughDstoresException();
            }
            if (fileIndex.containsKey(name) && !(fileIndex.get(name)
                == Index.STORE_IN_PROGRESS)) {
                throw new FileAlreadyExistsException(name);
            }
            Integer[] dstores = getStoreDStores();
            String message = "STORE_TO ";
            for (Integer num : dstores) {
                message += num + " ";
            }
            message.trim();
            return message;
        }

    }

    /**
     * This method makes the current thread wait until the rebalanceLock is released.
     * If the thread is interrupted while waiting, it prints the stack trace of the InterruptedException.
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
     * This method removes all existence of a file from the various file-related data structures.
     *
     * @param fileName The name of the file to be removed.
     */
    public void removeFileExistance(String fileName) {
        synchronized (fileLock) {
            setFileIndex(fileName, Index.REMOVE_COMPLETE);
            removeFileDstoreMap(fileName);
            removeFileSizeMap(fileName);
            removeFileLoadCount(fileName);
            removeFileFileList(fileName);
            removeDstoreFilemap(fileName);
            fileLoadRecord.remove(fileName);
            removeOccuringDstoreList(0);
        }
    }

    /**
     * This private method is used to remove a file from the dstoreFileMap.
     *
     * @param fileName The name of the file to be removed.
     */
    private void removeDstoreFilemap(String fileName) {
        synchronized (fileLock) {
            for (Integer dstore : dstoreList) {
                if (dstoreFileMap.containsKey(dstore)) {
                    dstoreFileMap.get(dstore).remove(fileName);
                }
            }
        }
    }
    /**
     * This method removes a file from the fileList.
     *
     * @param fileName The name of the file to be removed.
     */
    public void removeFileFileList(String fileName) {
        synchronized (fileLock) {
            fileList.remove(fileName);
        }

    }
    /**
     * This method removes a file from the fileDstoreMap.
     *
     * @param fileName The name of the file to be removed.
     */
    public void removeFileDstoreMap(String fileName) {
        synchronized (fileLock) {
            fileDstoreMap.remove(fileName);
        }
    }
    /**
     * This method removes a file from the fileSizeMap.
     *
     * @param fileName The name of the file to be removed.
     */
    public void removeFileSizeMap(String fileName) {
        synchronized (fileLock) {
            fileSizeMap.remove(fileName);
        }

    }

    private Object listLock = new Object();
    /**
     * This method makes the current thread wait until the listLock is released.
     *
     * @param port The port number to be used.
     */
    public void listWait(int port) {
        synchronized (listLock) {
            try {
                System.out.println("List wait");
                setListWaitFlag(port, true);
                listLock.wait();
                System.out.println("List wait over");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * This method notifies all threads waiting on the listLock object.
     */
    public void listStart() {
        synchronized (listLock) {
            System.out.println("List start");
            listLock.notifyAll();
        }
    }
    /**
     * This method removes a file's load count from the fileLoadRecord.
     *
     * @param fileName The name of the file to be removed.
     */
    public void removeFileLoadCount(String fileName) {
        synchronized (fileLock) {
            fileLoadRecord.remove(fileName);
        }
    }
    /**
     * This method updates the file index after a file is completely stored and
     * adds the file to the fileList.
     *
     * @param file The name of the file that has been stored.
     */
    public void storeFinished(String file) {
        synchronized (fileLock) {
            System.out.println("Store complete");
            setFileIndex(file, Index.STORE_COMPLETE);
            fileList.add(file);
            removeOccuringDstoreList(0);
        }
    }
    /**
     * This method checks if a file is present in the file index.
     *
     * @param file The name of the file to check.
     * @return True if the file is present in the file index, false otherwise.
     */
    public boolean checkIndexPresent(String file) {
        synchronized (fileLock) {
            return fileIndex.containsKey(file);
        }
    }
    /**
     * This method begins the removal process for a given file.
     *
     * @param file The name of the file to remove.
     */
    public void removeStart(String file) {
        synchronized (removeLock) {
            removeFile = file;
            removeLock.notifyAll();


            System.out.println("Remove start");
        }
    }

    /**
     * This method makes the current thread wait until the removeLock is released and then returns the name of the file to remove.
     *
     * @return The name of the file to remove.
     */
    public String removeWait() {
        synchronized (removeLock) {
            try {
                removeLock.wait();
                System.out.println("Remove wait done");
                return removeFile;
            } catch (InterruptedException e) {
                e.printStackTrace();
                return null;
            }
        }
    }


    /**
     * This method returns the replication factor.
     *
     * @return The replication factor.
     */
    public int getRepFactor() {
        return repFactor;
    }


    /**
     * Gets the value of the time out period.
     *
     * @return the value of time out period.
     */
    public int getTimeOut() {
        return timeOut;
    }


    /**
     * Gets the time required for rebalancing.
     *
     * @return the time required for rebalancing.
     */
    public int getRebalanceTime() {
        return rebalanceTime;
    }


    /**
     * Sets the index for the provided file. Thread-safe.
     *
     * @param file The file for which the index is to be set.
     * @param index The index to be set.
     */
    public void setFileIndex(String file, Index index) {
        synchronized (fileLock) {
            fileIndex.put(file, index);
        }

    }



    /**
     * Adds a datastore to the list of datastores. Thread-safe.
     *
     * @param dstore The datastore to be added.
     */
    public void addDstore(int dstore) {
        synchronized (fileLock) {

            dstoreList.add(dstore);
            System.out.println("Dstore added"+dstore);
        }
    }


    /**
     * Gets all the datastores that contain the provided file. Thread-safe.
     *
     * @param file The file to check the datastores for.
     * @return A list of all datastores that contain the file.
     */
    public ArrayList<Integer> getDstorewithFile(String file) {
        synchronized (fileLock) {
            ArrayList<Integer> dstores = new ArrayList<Integer>();
            for (Integer dstore : dstoreList) {
                if (dstoreFileMap.get(dstore).contains(file)) {
                    dstores.add(dstore);
                }
            }
            return dstores;
        }
    }
    /**
     * Fetches the datastores where a file is stored and calculates its size.
     * Throws exceptions when file doesn't exist,
     * not enough datastores are available, or a datastore can't receive a file.
     * Thread-safe.
     *
     * @param s The filename.
     * @param port The port number.
     * @return Array containing the datastore ID and the file size.
     * @throws NotEnoughDstoresException If not enough datastores are available.
     * @throws FileDoesNotExistException If the file does not exist.
     * @throws DStoreCantRecieveException If the datastore can't receive the file.
     */
    public int[] getFileDStores(String s, int port)
        throws NotEnoughDstoresException, FileDoesNotExistException, DStoreCantRecieveException {
        synchronized (fileLock) {
            if (checkIndexInProgress(s, 3)) {
                System.out.println("Concurrency error");
                throw new FileDoesNotExistException();
            }
            System.out.println(fileDstoreMap);
            if (!fileIndex.containsKey(s)) {
                throw new FileDoesNotExistException();
            } else if (dstoreList.size() < repFactor) {
                throw new NotEnoughDstoresException();
            } else if (fileLoadRecord.containsKey(s + "?" + port) && fileLoadRecord.get(
                s + "?" + port).containsAll(fileDstoreMap.get(s))) {
                System.out.println(
                    "FileLoadRecord: " + fileLoadRecord.get(s + "?" + port));
                System.out.println("FileDstoreMap: " + fileDstoreMap.get(s));
                System.out.println(
                    "fileLoadRecord.get(s).containsAll(fileDstoreMap.get(s))"
                        + fileLoadRecord.get(
                        s + "?" + port).containsAll(fileDstoreMap.get(s)));
                throw new DStoreCantRecieveException();
            }
            int[] result = new int[2];
            if (!fileLoadRecord.containsKey(s + "?" + port)) {
                fileLoadRecord.put(s + "?" + port, new ArrayList<>());
            }
            System.out.println("FileLoadRecord: " + fileLoadRecord.get(s) + port);
            System.out.println("FileDstoreMap: " + fileDstoreMap.get(s));
            System.out.println(
                "fileLoadRecord.get(s).containsAll(fileDstoreMap.get(s))"
                    + fileLoadRecord.get(
                    s + "?" + port).containsAll(fileDstoreMap.get(s)));
            for (int elem : fileDstoreMap.get(s)) {
                if (!fileLoadRecord.getOrDefault(s + "?" + port, new ArrayList<>())
                    .contains(elem)) {
                    result[0] = elem;
                    break;
                }
            }
            result[1] = fileSizeMap.get(s);
            //TODO check this
            fileLoadRecord.get(s + "?" + port).add(result[0]);

            return result;
        }
    }

    /**
     * Removes the load time record of the file on the given port. Thread-safe.
     *
     * @param s The filename.
     * @param port The port number.
     */
    public void setFileLoadTimes(String s, int port) {
        synchronized (fileLock) {
            fileLoadRecord.remove(s + "?" + port);
        }
    }


    /**
     * Returns a string of files to be removed from a specific port. The result is formed as a string
     * where each file is separated by a space.
     *
     * @param port the port number to get the files from
     * @return a string of files to be removed. If there are no files to be removed, it returns an empty string.
     */
    public String getRemoveFiles(int port) {
        synchronized (rebalanceLock) {
            String result = "";
            if (!dstoreRemoveMap.containsKey(port)) {
                System.out.println("No files to remove");
                return "";
            }
            for (String file : dstoreRemoveMap.get(port)) {
                result += file + " ";
            }
            int size = dstoreRemoveMap.get(port).size();
            result = size + " " + result;
            result.trim();
            return result;
        }
    }
    /**
     * Returns a string of files to be sent from a specific port. The result is formed as a string
     * where each file and its associated dstores are separated by a space.
     *
     * @param port the port number to get the files from
     * @return a string of files to be sent. If there are no files to be sent, it returns an empty string.
     */
    public String getSendFiles(int port) {
        synchronized (fileLock) {
            synchronized (rebalanceLock) {
                String result = "";
                if (!new2DstoreFileMap.containsKey(port)) {
                    System.out.println("No files to send");
                    return "";
                }
                int counter = 0;

                for (String file : new2DstoreFileMap.get(port)) {
                    String tempResult = "";
                    if (dstoreNeedMap.containsKey(file)) {
                        counter++;
                        for (int dstore : dstoreNeedMap.get(file)) {
                            tempResult += dstore + " ";
                        }
                        tempResult = file + " " + dstoreNeedMap.get(file).size() + " "
                            + tempResult;
                        tempResult.trim();
                        result += tempResult + " ";
                    }
                }
                result = counter + " " + result;
                result.trim();
                return result;
            }
        }
    }
    /**
     * Rebalances the data across all dstores in the system. It first checks the system state, then
     * clears and rebuilds the file and dstore maps. Lastly, it performs a final system check.
     */
    public void rebalanceDStores() {
        synchronized (rebalanceLock) {
            System.out.println("Rebalancing dstores");
            systemCheck(1);
            newFileDstoreMap.clear();
            newDstoreFileMap.clear();

            for (String file : fileList) {
                getRebalanceDstores(file);
            }
            dstoreNeedMap.clear();
            dstoreRemoveMap.clear();
            systemCheck(2);

            createRemoveMap();
            createNeedMap();
            systemCheck(3);
            new2DstoreFileMap = new HashMap<>(dstoreFileMap);
            fileDstoreMap = new HashMap<>(newFileDstoreMap);
            dstoreFileMap = new HashMap<>(newDstoreFileMap);
            System.out.println("Rebalancing dstores done");
        }

    }
    /**
     * Performs a system check by logging various data structures at a specific point in the process.
     *
     * @param number the current step in the process for which the system check is being performed
     */
    void systemCheck(int number) {

         System.out.println("*************************************");
         System.out.println("FileList: " + number + fileList);
         System.out.println("FileDstoreMap: " + number + fileDstoreMap);
         System.out.println("newFileDstoreMap: " + number + newFileDstoreMap);
         System.out.println("DstoreFileMap: " + number + dstoreFileMap);
         System.out.println("newDstoreFileMap: " + number + newDstoreFileMap);
         System.out.println("fileSizeMap: " + number + fileSizeMap);
         System.out.println("DstoreList: " + number + dstoreList);
         System.out.println("DstoreNeedMap: " + number + dstoreNeedMap);
         System.out.println("DstoreRemoveMap: " + number + dstoreRemoveMap);
         System.out.println("FileIndex " + number + fileIndex);
         System.out.println("*************************************");


    }
    /**
     * Builds the dstoreNeedMap, which maps files to the list of dstores that need them.
     */

    private void createNeedMap() {
        for (int dstore : dstoreList) {
            if (newDstoreFileMap.containsKey(dstore)) {
                ArrayList<String> files = new ArrayList<>(newDstoreFileMap.get(dstore));
                for (String file : files) {
                    if (!dstoreFileMap.containsKey(dstore) || !dstoreFileMap.get(dstore)
                        .contains(file)) {
                        if (dstoreNeedMap.containsKey(file) && !dstoreNeedMap.get(file)
                            .contains(dstore)) {
                            dstoreNeedMap.get(file).add(dstore);
                        } else {
                            ArrayList<Integer> needFiles = new ArrayList<>();
                            needFiles.add(dstore);
                            dstoreNeedMap.put(file, needFiles);
                        }
                    }
                }
            }
        }
    }
    /**
     * Builds the dstoreRemoveMap, which maps files to the list of dstores that need to remove them.
     */
    private void createRemoveMap() {
        for (int dstore : dstoreFileMap.keySet()) {
            if (newDstoreFileMap.containsKey(dstore)) {
                ArrayList<String> files = new ArrayList<>(dstoreFileMap.get(dstore));
                for (String file : files) {
                    if (!newDstoreFileMap.get(dstore).contains(file)) {
                        if (dstoreRemoveMap.containsKey(dstore) && !dstoreRemoveMap.get(
                                dstore)
                            .contains(file)) {
                            dstoreRemoveMap.get(dstore).add(file);
                        } else if (!dstoreRemoveMap.containsKey(dstore)) {
                            ArrayList<String> removeFiles = new ArrayList<>();
                            removeFiles.add(file);
                            dstoreRemoveMap.put(dstore, removeFiles);
                        }
                    }
                }
            } else {
                //dstoreRemoveMap.put(dstore, dstoreFileMap.get(dstore));
            }
        }

    }
    /**
     * Checks whether a file operation is in progress. The operations are defined by an integer command
     * where 1 represents removal and 2 represents storage.
     *
     * @param fileName the name of the file to check
     * @param command the operation to check
     * @throws NotEnoughDstoresException if there are not enough dstores available
     * @return true if the operation is in progress, false otherwise
     */
    public boolean checkIndexInProgress(String fileName, int command)
        throws NotEnoughDstoresException {
        synchronized (fileLock) {
            boolean result = (fileIndex.get(fileName) == Index.REMOVE_IN_PROGRESS
                || fileIndex.get(fileName) == Index.STORE_IN_PROGRESS);
            if (!result) {
                if (command == 1 && checkFile(fileName)) {
                    fileIndex.put(fileName, Index.REMOVE_IN_PROGRESS);
                } else if (command == 2) {
                    fileIndex.put(fileName, Index.STORE_IN_PROGRESS);
                }
            }

            return result;
        }
    }
    /**
     * Determines the appropriate dstores to store a file based on the current state of the system.
     *
     * @param file the name of the file to be stored
     */
    public void getRebalanceDstores(String file) {

        ArrayList<Integer> currentDstores = new ArrayList<>();
        ArrayList<Integer> allDstores = new ArrayList<>(dstoreFileMap.keySet());
        for (int k = 0; k < repFactor; k++) {
            Integer minDstore = getMinDstore(allDstores);
            currentDstores.add(minDstore);
            allDstores.remove(minDstore);
        }
        newFileDstoreMap.put(file, currentDstores);
        for (int dstore : currentDstores) {
            if (newDstoreFileMap.containsKey(dstore)) {
                newDstoreFileMap.get(dstore).add(file);
            } else {
                ArrayList<String> files = new ArrayList<>();
                files.add(file);
                newDstoreFileMap.put(dstore, files);
            }
        }


    }
    /**
     * Determines the dstore currently holding the minimum number of files.
     *
     * @param dstoreList list of available dstores
     * @return the dstore currently holding the minimum number of files
     */
    public Integer getMinDstore(ArrayList<Integer> dstoreList) {
        //TODO start again stores
        Integer min = newDstoreFileMap.getOrDefault(dstoreList.get(0),
            new ArrayList<>()).size();
        Integer minDstore = dstoreList.get(0);
        for (int dstore : dstoreList) {
            if (newDstoreFileMap.getOrDefault(dstore, new ArrayList<>()).size() < min) {
                min = newDstoreFileMap.getOrDefault(dstore, new ArrayList<>()).size();
                minDstore = dstore;
            }
        }
        return minDstore;
    }
    /**
     * Completes the rebalancing operation. It clears the reloadAck list and reschedules the next rebalancing operation.
     */
    public void rebalanceComplete() {
        synchronized (fileLock) {
            if (getRebalanveTakingPlace()) {

                reloadAck.add(1);
            }
            System.out.println("Reload ack size: " + reloadAck.size());
            System.out.println("Dstore list size: " + rebalanceDstoreList.size());
            if (reloadAck.size() == rebalanceDstoreList.size()) {
                for (String file : fileList) {
                    if (fileIndex.get(file) == Index.REMOVE_IN_PROGRESS) {
                        setFileIndex(file, Index.REMOVE_COMPLETE);
                        removeFileDstoreMap(file);
                        removeFileSizeMap(file);
                        removeFileLoadCount(file);
                        removeFileFileList(file);
                        removeDstoreFilemap(file);
                        fileLoadRecord.remove(file);
                    }
                }
                reloadAck.clear();
                setRebalanveTakingPlace(false);
                isRebalanceNotify();
                timer= new Timer();
                timer.scheduleAtFixedRate(new TimerTask() {
                    @Override
                    public void run() {
                        System.out.println("Rebalance timer started");
                        rebalanceStart();

                    }
                },getRebalanceTime(),getRebalanceTime());
                System.out.println("Rebalance complete");
            }

        }
    }

    /**
     * Handles the STORE_ACK command from a dstore. This method is called after a dstore confirms it has successfully stored a file.
     *
     * @param line the command from the dstore
     * @param port the port number of the dstore
     */
    public void dstoreStoreAckCommmand(String line, int port) {
        synchronized (fileLock) {
            if (checkIndexPresent(line.split(" ")[1])) {
                String fileName = line.split(" ")[1];
                updateFileDstores(fileName, port);
                storeLatchMap.get(fileName).countDown();
            }
        }
    }
    /**
     * Handles the "remove acknowledge" command from a dstore.
     *
     * @param line the line of input from the dstore.
     */
    public void dstoreRemoveAckCommmand(String line) {
        synchronized (fileLock) {
            String fileName = line.split(" ")[1];
            removeLatchMap.get(fileName).countDown();
        }
    }
    /**
     * Checks if a rebalance operation is currently taking place. If so, it waits
     * for the operation to complete before continuing.
     */
    public void checkRebalanceTakingPlace() {
        synchronized (fileLock) {
            if (getRebalanveTakingPlace()) {
                System.out.println("Rebalance taking place");
                isRebalanceWait();
            }
        }
    }
    /**
     * Handles a "store" command from a client. This method checks if there are enough dstores,
     * if the file already exists, and if there is a store operation in progress for the given file.
     * If none of these conditions are met, it updates the file size and sends a "store to" command.
     *
     * @param fileName the name of the file to store.
     * @param fileSize the size of the file to store.
     * @return a message to be sent to the client.
     * @throws FileAlreadyExistsException if the file already exists.
     * @throws NotEnoughDstoresException if there are not enough dstores available.
     */
    public String clientStoreCommand(String fileName, String fileSize)
        throws FileAlreadyExistsException, NotEnoughDstoresException {
        synchronized (fileLock) {
            if (dstoreList.size() < repFactor) {
                throw new NotEnoughDstoresException();
            }
            if (checkFile(fileName)) {
                System.out.println("File already exists");
                throw new FileAlreadyExistsException(fileName);
            }
            if (checkIndexInProgress(fileName, 2)) {
                System.out.println("Concurrency error");
                throw new FileAlreadyExistsException(fileName);
            }

            updateFileSize(fileName, Integer.parseInt(fileSize));
            String message = null;
            message = storeTo(fileName);
            putOccuringDstoreList(0);
            return message;


        }

    }
    /**
     * Handles a "remove" command from a client. This method checks if a remove operation
     * is in progress for the given file and if the file exists. If the file exists and no
     * operation is in progress, it removes the file from the file list.
     *
     * @param fileName the name of the file to remove.
     * @throws FileDoesNotExistException if the file does not exist.
     * @throws NotEnoughDstoresException if there are not enough dstores available.
     */
    public void clientRemoveCommand(String fileName)
        throws FileDoesNotExistException, NotEnoughDstoresException {
        synchronized (fileLock) {
            if (checkIndexInProgress(fileName, 1)) {
                System.out.println("Concurrency error");
                throw new FileDoesNotExistException();
            } else if (checkFile(fileName)) {
                removeFileFileList(fileName);
                putOccuringDstoreList(0);
            } else {
                System.out.println("File does not exist");
                throw new FileDoesNotExistException();
            }
        }
    }
    /**
     * Removes a file from a specific dstore. It removes the dstore from the file's list of dstores.
     * If the file has no more dstores, it also removes the file from the file list.
     *
     * @param port the port number of the dstore to remove the file from.
     */
    public void removeDstoreFileDstore(int port) {
        synchronized (fileLock) {
            if (dstoreFileMap.containsKey(port)) {
                ArrayList<String> files = new ArrayList<>(dstoreFileMap.get(port));
                for (String file : files) {
                    if (fileDstoreMap.containsKey(file)) {
                        ArrayList<Integer> dstores = new ArrayList<>(
                            fileDstoreMap.get(file));
                        dstores.remove((Integer) port);
                        System.out.println("Dstores size: " + dstores.size());
                        System.out.println("File: " + file);
                        if (dstores.size() == 0) {
                            removeFileFileList(file);
                        }
                        fileDstoreMap.put(file, dstores);
                    }
                }
            }
        }
    }
    /**
     * Removes a dstore from the dstore file map.
     *
     * @param port the port number of the dstore to remove.
     */
    public void removeDstoreDstoreFileMap(int port) {
        synchronized (fileLock) {
            if (dstoreFileMap.containsKey(port)) {
                dstoreFileMap.remove((Integer) port);
            }
        }
    }
    /**
     * Removes a dstore from the reload lists.
     *
     * @param port the port number of the dstore to remove.
     */
    public void removeDstoreReloadLists(int port) {
        synchronized (fileLock) {
            if (dstoreRemoveMap.containsKey(port)) {
                dstoreRemoveMap.remove((Integer) port);
            }
            if (dstoreNeedMap.containsKey(port)) {
                dstoreNeedMap.remove((Integer) port);
            }
        }
    }

    /**
     * Removes a dstore from various data structures. It removes the dstore from the dstore list,
     * the file dstore map, the dstore file map, the reload lists, and the wait flag list.
     *
     * @param port the port number of the dstore to remove.
     */
    public void removeDstore(int port) {
        synchronized (fileLock) {
            removeDstoreList(port);
            removeDstoreFileDstore(port);
            removeDstoreDstoreFileMap(port);
            removeDstoreReloadLists(port);
            removeListWaitFlag(port);
        }
    }
    /**
     * Removes a dstore from the list of dstores waiting for a response.
     *
     * @param port the port number of the dstore to remove.
     */
    public void removeListWaitFlag(int port) {
        synchronized (fileLock) {
            if (listWaitFlag.containsKey(port)) {
                listWaitFlag.remove((Integer) port);
            }
        }
    }

    /**
     * Handles a "remove failed" command. This method removes the first dstore from
     * the list of dstores where an operation is currently occurring.
     *
     * @param fileName the name of the file that failed to be removed.
     */

    public void removeFailed(String fileName) {
        synchronized(fileLock){
            removeOccuringDstoreList(0);
        }
    }
}

