import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.HashMap;


public class ControllerInfo {

    private int cport = 0;
    private int repFactor = 0;
    private int timeOut = 0;
    private int rebalanceTime = 0;
    private ArrayList<Integer> dstoreList = new ArrayList<Integer>();
    private ArrayList<String> fileList = new ArrayList<String>();
    private HashMap<String, ArrayList<Integer>> fileDstoreMap = new HashMap<String, ArrayList<Integer>>();
    private HashMap<String, Index> fileIndex = new HashMap<String, Index>();
    private HashMap<String, ArrayList<Integer>> storeAcks = new HashMap<String, ArrayList<Integer>>();
    private HashMap<String, Integer> fileSizeMap = new HashMap<String, Integer>();
    private HashMap<String, ArrayList<Integer>> removeAcks = new HashMap<String,
        ArrayList<Integer>>();

    private HashMap<String, Integer> fileLoadCount = new HashMap<String, Integer>();
    private Object storeLock = new Object();

    private Object removeLock = new Object();

    private Object removeAckLock = new Object();

    private String removeFile = null;

    boolean removeAckFlag = true;

    boolean removeFlag = true;



    public boolean checkFile (String fileName) throws NotEnoughDstoresException {
        if (dstoreList.size() < repFactor) {
            throw new NotEnoughDstoresException();
        }
        return fileIndex.containsKey(fileName);
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
        if (fileDstoreMap.containsKey(fileName)) {
            fileDstoreMap.get(fileName).add(dstore);
        } else {
            ArrayList<Integer> dstores = new ArrayList<Integer>();
            dstores.add(dstore);
            fileDstoreMap.put(fileName, dstores);
        }
    }

    public void updateFileSize(String fileName, Integer size) {
        fileSizeMap.put(fileName, size);
    }


    public Integer[] getStoreDStores() {
        Integer[] dstores = new Integer[repFactor];
        dstores = dstoreList.subList(0, repFactor).toArray(dstores);
        return dstores;
    }

    public String getRemoveFile() {
        return removeFile;
    }


    public String list() throws NotEnoughDstoresException {
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

    public String storeTo(String name)
        throws NotEnoughDstoresException, FileAlreadyExistsException {
        if (dstoreList.size() < repFactor) {
            throw new NotEnoughDstoresException();
        }
        if (fileList.contains(name)) {
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

    public void removeAck(String fileName) {
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
            removeAckStart();
        }
    }


    public void storeAck(String file) {
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
        fileIndex.put(file, index);
    }

    public Index getFileIndex(String file) {
        return fileIndex.get(file);
    }

    public ArrayList<Integer> getDstoreList() {
        return dstoreList;
    }

    public void setDstoreList(ArrayList<Integer> dstoreList) {
        this.dstoreList = dstoreList;
    }

    public void addDstore(int dstore) {
        dstoreList.add(dstore);
    }

    public void setCport(int cport) {
        this.cport = cport;
    }

    public int[] getFileDStores(String s, int times)
        throws NotEnoughDstoresException, FileDoesNotExistException, DStoreCantRecieveException {
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

    public int getFileLoadTimes (String s) {
        fileLoadCount.put(s, fileLoadCount.getOrDefault(s, 0) + 1);
        return fileLoadCount.get(s);
    }


}

