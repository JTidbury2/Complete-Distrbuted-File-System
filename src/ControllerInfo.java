import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.HashMap;


public class ControllerInfo {

    private int cport = 0;
    private int repFactor = 0;
    private int timeOut = 0;
    private int rebalanceTime = 0;

    private boolean listFlag = true;
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

    private HashMap<Integer, ArrayList<Integer>> dstoreAddMap = new HashMap<Integer, ArrayList<Integer>>();

    HashMap<String, ArrayList<Integer>> dstoreNeedMap = new HashMap<>();

    private HashMap<String, Index> fileIndex = new HashMap<String, Index>();
    private HashMap<String, ArrayList<Integer>> storeAcks = new HashMap<String, ArrayList<Integer>>();
    private HashMap<String, Integer> fileSizeMap = new HashMap<String, Integer>();
    private HashMap<String, ArrayList<Integer>> removeAcks = new HashMap<String,
        ArrayList<Integer>>();

    private ArrayList<Integer> reloadAck = new ArrayList<>();

    private HashMap<String, Integer> fileLoadCount = new HashMap<String, Integer>();
    private Object storeLock = new Object();

    private Object joinLock = new Object();

    private Object removeLock = new Object();

    private Object removeAckLock = new Object();

    private Object rebalanceLock = new Object();

    private String removeFile = null;

    boolean removeAckFlag = true;

    boolean removeFlag = true;

    public boolean isRebalanceFlag() {
        return rebalanceFlag;
    }

    public void setRebalanceFlag(boolean rebalanceFlag) {
        this.rebalanceFlag = rebalanceFlag;
    }

    boolean rebalanceFlag = true;

    Object fileLock = new Object();


    public boolean checkFile(String fileName) throws NotEnoughDstoresException {
        synchronized (fileLock) {
            if (dstoreList.size() < repFactor) {
                throw new NotEnoughDstoresException();
            }
            return fileIndex.containsKey(fileName);
        }
    }

    public boolean getRemoveAckFlag() {
        return removeAckFlag;
    }

    public void setRemoveAckFlag(boolean flag) {
        removeAckFlag = flag;
    }

    public boolean getRemoveFlag() {
        return removeFlag;
    }

    public void updateFileDstores(String fileName, Integer dstore) {
        synchronized (fileLock) {
            if (fileDstoreMap.containsKey(fileName)) {
                fileDstoreMap.get(fileName).add(dstore);
            } else {
                ArrayList<Integer> dstores = new ArrayList<Integer>();
                dstores.add(dstore);
                fileDstoreMap.put(fileName, dstores);
            }
            if (dstoreFileMap.containsKey(dstore)) {
                dstoreFileMap.get(dstore).add(fileName);
            } else {
                ArrayList<String> files = new ArrayList<String>();
                files.add(fileName);
                dstoreFileMap.put(dstore, files);
            }
        }
    }

    public boolean getListFlag() {
        return listFlag;
    }

    public void updateFileSize(String fileName, Integer size) {
        synchronized (fileLock) {
            fileSizeMap.put(fileName, size);
        }
    }


    public Integer[] getStoreDStores() {
        synchronized (fileLock) {
            Integer[] dstores = new Integer[repFactor];
            dstores = dstoreList.subList(0, repFactor).toArray(dstores);
            return dstores;
        }
    }

    public String getRemoveFile() {
        return removeFile;
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
            if (fileIndex.containsKey(name)) {
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






    public void rebalanceStart() {
        synchronized (rebalanceLock) {
            rebalanceDStores();
            rebalanceLock.notifyAll();
        }
    }

    public void rebalanceWait() {
        synchronized (rebalanceLock) {
            try {
                rebalanceLock.wait();
                System.out.println("Stats");
                System.out.println("Dstore list: " + dstoreList);
                System.out.println("File list: " + fileList);
                System.out.println("File dstore map: " + fileDstoreMap);
                System.out.println("Dstore file map: " + dstoreFileMap);
                System.out.println("New file dstore map: " + newFileDstoreMap);
                System.out.println("New dstore file map: " + newDstoreFileMap);
                System.out.println("Dstore remove map: " + dstoreRemoveMap);
                System.out.println("Dstore add map: " + dstoreAddMap);
                System.out.println("Dstore need map: " + dstoreNeedMap);
                System.out.println("File index: " + fileIndex);

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


    public void removeAck(String fileName) {
        synchronized (fileLock) {
            if (removeAcks.containsKey(fileName)) {
                removeAcks.get(fileName).add(1);
            } else {
                ArrayList<Integer> acks = new ArrayList<Integer>();
                acks.add(1);
                removeAcks.put(fileName, acks);
            }
            if (removeAcks.get(fileName).size() == repFactor) {
                System.out.println("Remove complete");
                removeAcks.clear();
                setFileIndex(fileName, Index.REMOVE_COMPLETE);
                fileList.remove(fileName);
                fileIndex.remove(fileName);
                fileDstoreMap.remove(fileName);
                fileSizeMap.remove(fileName);
                fileLoadCount.remove(fileName);
                for (Integer dstore : dstoreList) {
                    if (dstoreFileMap.containsKey(dstore)) {
                        dstoreFileMap.get(dstore).remove(fileName);
                    }
                }
                removeAckStart();
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

    public void removeFileLoadCount(String fileName) {
        synchronized (fileLock) {
            fileLoadCount.remove(fileName);
        }
    }


    public void storeAck(String file) {
        synchronized (fileLock) {
            if (storeAcks.containsKey(file)) {
                storeAcks.get(file).add(1);
            } else {
                ArrayList<Integer> acks = new ArrayList<Integer>();
                acks.add(1);
                storeAcks.put(file, acks);
            }
            if (storeAcks.get(file).size() == repFactor) {
                System.out.println("Store complete");
                storeAcks.clear();
                setFileIndex(file, Index.STORE_COMPLETE);
                fileList.add(file);
                storeComplete();
            }
        }
    }

    public void removeStart(String file) {
        synchronized (removeLock) {
            removeFile = file;
            removeLock.notifyAll();

            System.out.println("Remove start");
        }
    }


    public void removeWait() {
        synchronized (removeLock) {
            try {
                removeLock.wait();
                System.out.println("Remove wait done");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void removeAckWait() {
        synchronized (removeAckLock) {
            try {
                removeAckLock.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    private void removeAckStart() {
        synchronized (removeAckLock) {
            removeAckLock.notifyAll();
        }
    }

    public void storeComplete() {
        synchronized (storeLock) {
            storeLock.notifyAll();
        }
    }

    public void storeWait() {
        synchronized (storeLock) {
            try {
                storeLock.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
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

    public ArrayList<Integer> getDstoreList() {
        return dstoreList;
    }

    public void setDstoreList(ArrayList<Integer> dstoreList) {
        this.dstoreList = dstoreList;
    }

    public void addDstore(int dstore) {
        synchronized (fileLock) {
            dstoreList.add(dstore);
        }
    }

    public void setCport(int cport) {
        this.cport = cport;
    }

    public int[] getFileDStores(String s, int times)
        throws NotEnoughDstoresException, FileDoesNotExistException, DStoreCantRecieveException {
        synchronized (fileLock) {
            System.out.println(fileDstoreMap);
            times = times - 1;
            if (!fileIndex.containsKey(s)) {
                throw new FileDoesNotExistException();
            } else if (dstoreList.size() < repFactor) {
                throw new NotEnoughDstoresException();
            } else if (fileDstoreMap.get(s).size() == times) {
                throw new DStoreCantRecieveException();
            }
            System.out.println("File: " + s + " Dstores: " + fileDstoreMap.get(s).get(times));
            int[] result = new int[2];
            result[0] = fileDstoreMap.get(s).get(times);
            result[1] = fileSizeMap.get(s);
            return result;
        }
    }

    public int getFileLoadTimes(String s) {
        synchronized (fileLock) {
            fileLoadCount.put(s, fileLoadCount.getOrDefault(s, 0) + 1);
            return fileLoadCount.get(s);
        }
    }

    public void setFileLoadTimes (String s, int times) {
        synchronized (fileLock) {
            fileLoadCount.put(s, times);
        }
    }


    public void updateDstoreFiles(String[] files, int port) {
        synchronized (fileLock) {
            for (String file : files) {
                if (fileDstoreMap.containsKey(file) && !fileDstoreMap.get(file).contains(port)) {
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

    public void removeDstoreFiles(String[] files, int port) {
        synchronized (fileLock) {
            for (String file : files) {
                if (fileDstoreMap.containsKey(file) && fileDstoreMap.get(file).contains(port)) {
                    fileDstoreMap.get(file).remove((Integer) port);
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
                System.out.println("DstoreFileMap: " + new2DstoreFileMap);
                System.out.println("DstoreNeedMap: " + dstoreNeedMap);
                for (String file : new2DstoreFileMap.get(port)) {
                    String tempResult = "";
                    if (dstoreNeedMap.containsKey(file)) {
                        counter++;
                        for (int dstore : dstoreNeedMap.get(file)) {
                            tempResult += dstore + " ";
                        }
                        tempResult = file + " " + dstoreNeedMap.get(file).size() + " " + tempResult;
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
            systemCheck(1);
            System.out.println("Rebalancing dstores");

            newFileDstoreMap.clear();
            systemCheck(2);
            newDstoreFileMap.clear();
            systemCheck(3);
            for (String file : fileList) {
                getRebalanceDstores(file);
            }
            systemCheck(4);
            createRemoveMap();
            createNeedMap();
            systemCheck(5);
            new2DstoreFileMap = new HashMap<>(dstoreFileMap);
            fileDstoreMap = new HashMap<>(newFileDstoreMap);
            dstoreFileMap = new HashMap<>(newDstoreFileMap);
            systemCheck(6);
        }

    }

    private void systemCheck(int number){
        System.out.println("*************************************");
        System.out.println("FileList: "+number + fileList);
        System.out.println("FileDstoreMap: "+number  + fileDstoreMap);
        System.out.println("newFileDstoreMap: "+number  + newFileDstoreMap);
        System.out.println("DstoreFileMap: "+number  + dstoreFileMap);
        System.out.println("newDstoreFileMap: "+number  + newDstoreFileMap);
        System.out.println("fileSizeMap: "+number  + fileSizeMap);
        System.out.println("DstoreList: "+number  + dstoreList);
        System.out.println("DstoreNeedMap: "+number  + dstoreNeedMap);
        System.out.println("DstoreRemoveMap: "+number  + dstoreRemoveMap);
        System.out.println("*************************************");

    }


    private void createNeedMap() {
        for (int dstore : dstoreList) {
            if (newDstoreFileMap.containsKey(dstore)) {
                ArrayList<String> files = new ArrayList<>(newDstoreFileMap.get(dstore));
                for (String file : files) {
                    if (!dstoreFileMap.containsKey(dstore)||!dstoreFileMap.get(dstore).contains(file) ) {
                        if (dstoreNeedMap.containsKey(file)) {
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
        System.out.println("££££££0000000000000000000000000 Notice me");
        System.out.println(dstoreList);
        System.out.println(newDstoreFileMap);
        System.out.println(dstoreFileMap);
        for (int dstore : dstoreFileMap.keySet()) {
            System.out.println("££££££ Notice me");
            if (newDstoreFileMap.containsKey(dstore)) {
                System.out.println("88888888 Notice me");
                ArrayList<String> files = new ArrayList<>(dstoreFileMap.get(dstore));
                for (String file : files) {
                    System.out.println("777777777 Notice me");
                    if (!newDstoreFileMap.get(dstore).contains(file)) {
                        if (dstoreRemoveMap.containsKey(dstore)) {
                            dstoreRemoveMap.get(dstore).add(file);
                        } else {
                            ArrayList<String> removeFiles = new ArrayList<>();
                            removeFiles.add(file);
                            dstoreRemoveMap.put(dstore, removeFiles);
                        }
                    }
                }
            }
        }
        System.out.println(dstoreRemoveMap);
    }

    public void getRebalanceDstores(String file) {

        ArrayList<Integer> currentDstores = new ArrayList<>();
        ArrayList<Integer> allDstores = new ArrayList<>(dstoreList);
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
        Integer min = newDstoreFileMap.getOrDefault(dstoreList.get(0), new ArrayList<>()).size();
        Integer minDstore = dstoreList.get(0);
        for (int dstore : dstoreList) {
            if (newDstoreFileMap.getOrDefault(dstore, new ArrayList<>()).size() < min) {
                min = newDstoreFileMap.getOrDefault(dstore, new ArrayList<>()).size();
                minDstore = dstore;
            }
        }
        return minDstore;
    }
}

