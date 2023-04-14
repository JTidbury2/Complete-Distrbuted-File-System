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

    private HashMap<Integer,ArrayList<String>> dstoreFileMap = new HashMap<Integer,ArrayList<String>>();
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

    Object fileLock = new Object();



    public boolean checkFile (String fileName) throws NotEnoughDstoresException {
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
        }
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


    public String  list() throws NotEnoughDstoresException {
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
                removeAckStart();
            }

        }
    }

    public void removeFileFileList(String fileName) {
        synchronized (fileLock){
            fileList.remove(fileName);
        }

    }

    public void removeFileDstoreMap(String fileName) {
        synchronized (fileLock){
            fileDstoreMap.remove(fileName);
        }
    }

    public void removeFileSizeMap(String fileName) {
        synchronized (fileLock){
            fileSizeMap.remove(fileName);
        }

    }

    public void removeFileLoadCount (String fileName) {
        synchronized (fileLock){
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
        synchronized (fileLock){
            fileIndex.put(file, index);
        }

    }

    public Index getFileIndex(String file) {
        synchronized (fileLock){
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
        synchronized (fileLock){
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

    public int getFileLoadTimes (String s) {
        synchronized (fileLock) {
            fileLoadCount.put(s, fileLoadCount.getOrDefault(s, 0) + 1);
            return fileLoadCount.get(s);
        }
    }


    public void updateDstoreFiles(String[] files, int port) {
        synchronized (fileLock) {
            for (String file : files) {
                if (fileDstoreMap.containsKey(file) && !fileDstoreMap.get(file).contains(port)){
                    fileDstoreMap.get(file).add(port);
                } else {
                    ArrayList<Integer> dstores = new ArrayList<Integer>();
                    dstores.add(port);
                    fileDstoreMap.put(file, dstores);
                }

            }
        }
    }

    public void removeDstoreFiles(String[] files, int port) {
        synchronized (fileLock) {
            for (String file : files) {
                if (fileDstoreMap.containsKey(file) && fileDstoreMap.get(file).contains(port)){
                    fileDstoreMap.get(file).remove((Integer) port);
                }
            }
        }
    }

    public void rebalanceDStores(){
        fileDstoreMap.clear();
        dstoreFileMap.clear();
        for (String file : fileList) {
            getRebalanceDstores(file);
        }

        }

    public void getRebalanceDstores(String file){

        ArrayList<Integer> currentDstores = new ArrayList<>();
        ArrayList<Integer> allDstores = new ArrayList<>(dstoreList);
        for (int k = 0; k<repFactor;k++){
            Integer minDstore = getMinDstore(allDstores);
            currentDstores.add(minDstore);
            allDstores.remove(minDstore);
        }
        fileDstoreMap.put(file, currentDstores);
        for (int dstore:currentDstores){
            if (dstoreFileMap.containsKey(dstore)){
                dstoreFileMap.get(dstore).add(file);
            } else {
                ArrayList<String> files = new ArrayList<>();
                files.add(file);
                dstoreFileMap.put(dstore, files);
            }
        }



    }

    public Integer getMinDstore(ArrayList<Integer> dstoreList){
        //TODO start again stores
        Integer min = dstoreFileMap.getOrDefault(dstoreList.get(0),new ArrayList<>()).size();
        Integer minDstore = dstoreList.get(0);
        for (int dstore:dstoreList){
            if (dstoreFileMap.get(dstore).size() < min){
                min = dstoreFileMap.get(dstore).size();
                minDstore = dstore;
            }
        }
        return minDstore;
    }
}

