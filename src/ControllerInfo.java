import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class ControllerInfo {

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

    boolean removeFlag = true;
    boolean rebalanceFlag = true;

    CountDownLatch rebalanceLatch;

    Object fileLock = new Object();

    public void setListWaitFlag(int port, boolean flag) {
        synchronized (fileLock) {
            listWaitFlag.put(port, flag);
        }
    }

    public boolean getListWaitFlag() {
        synchronized (fileLock) {
            System.out.println("listWaitFlag is " + listWaitFlag);
            boolean temp = listWaitFlag.containsValue(false);
            return (!temp);
        }
    }

    public void addRemoveLatchMap(String file, CountDownLatch latch) {
        synchronized (fileLock) {
            removeLatchMap.put(file, latch);
        }
    }

    public void removeRemoveLatchMap(String file) {
        synchronized (fileLock) {
            removeLatchMap.remove(file);
        }
    }

    public void addStoreLatchMap(String file, CountDownLatch latch) {
        synchronized (fileLock) {
            storeLatchMap.put(file, latch);
        }
    }

    public void removeStoreLatchMap(String file) {
        synchronized (fileLock) {
            storeLatchMap.remove(file);
        }
    }

    public void removeDstoreList(int port) {
        synchronized (fileLock) {
            dstoreList.remove((Integer) port);
        }
    }

    public void addDstoreList(int port) {
        synchronized (fileLock) {
            dstoreList.add(port);
        }
    }

    public boolean isRebalanceFlag() {
        synchronized (fileLock) {
            return rebalanceFlag;
        }
    }


    public boolean getRebalanveTakingPlace() {
        synchronized (fileLock) {
            return rebalanveTakingPlace;
        }

    }

    public void setRebalanveTakingPlace(boolean flag) {
        synchronized (fileLock) {
            rebalanveTakingPlace = flag;
        }
    }

    public void isRebalanceWait() {
        synchronized (isRebalanceLock) {
            try {
                isRebalanceLock.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    public void isRebalanceNotify() {
        synchronized (isRebalanceLock) {
            isRebalanceLock.notifyAll();
        }
    }

    public void removeFileIndex(String file) {
        synchronized (fileLock) {
            fileIndex.remove(file);

        }
    }

    public void storeFailed(String file) {
        synchronized (fileLock) {
            removeFileIndex(file);
            removeFileDstoreMap(file);
            removeDstoreFilemap(file);
        }
    }

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
        rebalanceLatch = new CountDownLatch(dstoreList.size());
        System.out.println("Rebalance latch count is " + rebalanceLatch.getCount());
        rebalanceFinished = true;
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
                                return;
                            }

                            updateRelevantMaps();
                            systemCheck(0);

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
                    //Don't include files without store complete
                    if (fileIndex.get(file) != Index.STORE_COMPLETE) {
                        continue;
                    }
                    //Update file list
                    if ((!fileList.contains(file))
                        && fileIndex.get(file) == Index.STORE_COMPLETE) {
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

    public void listRecieved(int port, ArrayList<String> files) {
        synchronized (fileLock) {
            if (rebalanceFinished) {
                listReturnMap.put(port, files);
                rebalanceLatch.countDown();
            }

        }
    }


    public void rebalance() {
        synchronized (fileLock) {
            synchronized (rebalanceLock) {
                System.out.println("REBALANCE STARTED");
                rebalanceDStores();
                rebalanceLock.notifyAll();
            }
        }
    }

    private boolean checkIndex() {
        synchronized (fileLock) {
            boolean result = false;
            if (fileIndex.size() == 0) {
                System.out.println("Index is empty");
                return true;
            }
            int count = 0;
            for (String fileName : fileIndex.keySet()) {
                if (!(fileIndex.get(fileName) == Index.STORE_IN_PROGRESS)
                    && !(fileIndex.get(fileName) == Index.REMOVE_IN_PROGRESS)) {
                    count++;
                }
            }
            if (count == fileIndex.size()) {
                result = true;
            }
            System.out.println("Index is" + fileIndex);
            System.out.println("Count is " + count);
            return result;
        }
    }


    public boolean checkFile(String fileName) throws NotEnoughDstoresException {
        synchronized (fileLock) {
            if (dstoreList.size() < repFactor) {
                throw new NotEnoughDstoresException();
            }
            return fileList.contains(fileName);
        }
    }


    public boolean getRemoveFlag() {
        synchronized (fileLock) {
            return removeFlag;
        }
    }

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

    public void updateFileSize(String fileName, Integer size) {
        synchronized (fileLock) {
            fileSizeMap.put(fileName, size);
        }
    }


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


    public String getRemoveFile() {
        synchronized (fileLock) {
            return removeFile;
        }
    }


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


    public void rebalanceWait() {
        synchronized (rebalanceLock) {
            try {
                rebalanceLock.wait();

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void joinWait() {
        synchronized (joinLock) {
            try {
                joinLock.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void joinStart() {
        synchronized (joinLock) {
            joinLock.notifyAll();
        }
    }

    public void removeFileExistance(String fileName) {
        synchronized (fileLock) {
            setFileIndex(fileName, Index.REMOVE_COMPLETE);
            removeFileDstoreMap(fileName);
            removeFileSizeMap(fileName);
            removeFileLoadCount(fileName);
            removeFileFileList(fileName);
            removeDstoreFilemap(fileName);
            fileLoadRecord.remove(fileName);
        }
    }


    private void removeDstoreFilemap(String fileName) {
        synchronized (fileLock) {
            for (Integer dstore : dstoreList) {
                if (dstoreFileMap.containsKey(dstore)) {
                    dstoreFileMap.get(dstore).remove(fileName);
                }
            }
        }
    }

    public void removeFileFileList(String fileName) {
        synchronized (fileLock) {
            fileList.remove(fileName);
        }

    }

    public void removeFileDstoreMap(String fileName) {
        synchronized (fileLock) {
            fileDstoreMap.remove(fileName);
        }
    }

    public void removeFileSizeMap(String fileName) {
        synchronized (fileLock) {
            fileSizeMap.remove(fileName);
        }

    }

    private Object listLock = new Object();

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

    public void listStart() {
        synchronized (listLock) {
            System.out.println("List start");
            listLock.notifyAll();
        }
    }

    public void removeFileLoadCount(String fileName) {
        synchronized (fileLock) {
            fileLoadRecord.remove(fileName);
        }
    }

    public void storeFinished(String file) {
        synchronized (fileLock) {
            System.out.println("Store complete");
            setFileIndex(file, Index.STORE_COMPLETE);
            fileList.add(file);
        }
    }

    public boolean checkIndexPresent(String file) {
        synchronized (fileLock) {
            return fileIndex.containsKey(file);
        }
    }

    public void removeStart(String file) {
        synchronized (removeLock) {
            removeFile = file;
            removeLock.notifyAll();

            System.out.println("Remove start");
        }
    }


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


    public int getCport() {
        return cport;
    }

    public int getRepFactor() {
        return repFactor;
    }

    public void setRepFactor(int repFactor) {
        this.repFactor = repFactor;
    }

    public int getTimeOut() {
        return timeOut;
    }

    public void setTimeOut(int timeOut) {
        this.timeOut = timeOut;
    }

    public int getRebalanceTime() {
        return rebalanceTime;
    }

    public void setRebalanceTime(int rebalanceTime) {
        this.rebalanceTime = rebalanceTime;
    }

    public void setFileIndex(String file, Index index) {
        synchronized (fileLock) {
            fileIndex.put(file, index);
        }

    }

    public Index getFileIndex(String file) {
        synchronized (fileLock) {
            return fileIndex.get(file);
        }

    }


    public void addDstore(int dstore) {
        synchronized (fileLock) {

            dstoreList.add(dstore);
            System.out.println("Dstore added"+dstore);
        }
    }

    public void setCport(int cport) {
        this.cport = cport;
    }

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


    public void setFileLoadTimes(String s, int port) {
        synchronized (fileLock) {
            fileLoadRecord.remove(s + "?" + port);
        }
    }


    public void updateDstoreFiles(String[] files, int port) {
        synchronized (fileLock) {
            for (String file : files) {
                if (fileDstoreMap.containsKey(file) && !fileDstoreMap.get(file)
                    .contains(port)) {
                    fileDstoreMap.get(file).add(port);
                } else {
                    ArrayList<Integer> dstores = new ArrayList<Integer>();
                    dstores.add(port);
                    fileDstoreMap.put(file, dstores);
                }
                if (dstoreFileMap != null && dstoreFileMap.containsKey(port)) {
                    dstoreFileMap.get(port).add(file);
                } else {
                    ArrayList<String> filesList = new ArrayList<String>();
                    filesList.add(file);
                    dstoreFileMap.put(port, filesList);
                }


            }

        }
    }


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
            new2DstoreFileMap = new HashMap<>(dstoreFileMap);
            fileDstoreMap = new HashMap<>(newFileDstoreMap);
            dstoreFileMap = new HashMap<>(newDstoreFileMap);
            System.out.println("Rebalancing dstores done");
        }

    }

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
                System.out.println("Rebalance complete");
            }

        }
    }

    public void rebalanceTimout() {
        synchronized (fileLock) {
            if (getRebalanveTakingPlace()) {
                reloadAck.clear();
                setRebalanveTakingPlace(false);
                isRebalanceNotify();
                System.out.println("Rebalance timeout");
            }
        }
    }

    public void dstoreStoreAckCommmand(String line, int port) {
        synchronized (fileLock) {
            if (checkIndexPresent(line.split(" ")[1])) {
                String fileName = line.split(" ")[1];
                updateFileDstores(fileName, port);
                storeLatchMap.get(fileName).countDown();
            }
        }
    }

    public void dstoreRemoveAckCommmand(String line) {
        synchronized (fileLock) {
            String fileName = line.split(" ")[1];
            removeLatchMap.get(fileName).countDown();
        }
    }

    public void checkRebalanceTakingPlace() {
        synchronized (fileLock) {
            if (getRebalanveTakingPlace()) {
                System.out.println("Rebalance taking place");
                isRebalanceWait();
            }
        }
    }

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
            return message;


        }

    }

    public void clientRemoveCommand(String fileName)
        throws FileDoesNotExistException, NotEnoughDstoresException {
        synchronized (fileLock) {
            if (checkIndexInProgress(fileName, 1)) {
                System.out.println("Concurrency error");
                throw new FileDoesNotExistException();
            } else if (checkFile(fileName)) {
                removeFileFileList(fileName);
                removeStart(fileName);
            } else {
                System.out.println("File does not exist");
                throw new FileDoesNotExistException();
            }
        }
    }

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

    public void removeDstoreDstoreFileMap(int port) {
        synchronized (fileLock) {
            if (dstoreFileMap.containsKey(port)) {
                dstoreFileMap.remove((Integer) port);
            }
        }
    }

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


    public void removeDstore(int port) {
        synchronized (fileLock) {
            removeDstoreList(port);
            removeDstoreFileDstore(port);
            removeDstoreDstoreFileMap(port);
            removeDstoreReloadLists(port);
        }
    }


}

