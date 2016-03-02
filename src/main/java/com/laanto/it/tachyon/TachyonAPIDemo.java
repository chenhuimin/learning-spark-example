package com.laanto.it.tachyon;


import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.ClientContext;
import tachyon.client.TachyonStorageType;
import tachyon.client.UnderStorageType;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.InStreamOptions;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.FileAlreadyExistsException;
import tachyon.exception.TachyonException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;


public class TachyonAPIDemo {

    public static void main(String[] args) throws IOException, TachyonException {
        TachyonURI mMasterLocation = new TachyonURI("tachyon://node3.laanto.com:19998");
        TachyonConf mTachyonConf = ClientContext.getConf();
        mTachyonConf.set(Constants.MASTER_HOSTNAME, mMasterLocation.getHost());
        mTachyonConf.set(Constants.MASTER_PORT, Integer.toString(mMasterLocation.getPort()));
        mTachyonConf.set(Constants.ZOOKEEPER_ENABLED, "false");
        ClientContext.reset(mTachyonConf);
        TachyonFileSystem tfs = TachyonFileSystem.TachyonFileSystemFactory.get();
        read(tfs, new TachyonURI("/test/number.txt"));
        write(tfs, new TachyonURI("/test/number.txt"), true, 1000);


    }

    private static boolean read(TachyonFileSystem tachyonClient, TachyonURI mFilePath) throws IOException, TachyonException {
        InStreamOptions clientOptions = new InStreamOptions.Builder(ClientContext.getConf())
                .setTachyonStorageType(TachyonStorageType.STORE).build();
        TachyonFile file = tachyonClient.open(mFilePath);
        DataInputStream input = new DataInputStream(tachyonClient.getInStream(file, clientOptions));
        try {
            int length = input.readInt();
            for (int i = 0; i < length; i++) {
                if (input.readInt() != i) {
                    return false;
                }
            }
        } finally {
            input.close();
        }
        return true;
    }

    private static void write(TachyonFileSystem tachyonClient, TachyonURI mFilePath, Boolean mDeleteIfExists, Integer mLength) throws IOException, TachyonException {
        OutStreamOptions clientOptions = new OutStreamOptions.Builder(ClientContext.getConf())
                .setTachyonStorageType(TachyonStorageType.STORE).setUnderStorageType(UnderStorageType.NO_PERSIST).build();
        FileOutStream fileOutStream =
                getOrCreate(tachyonClient, mFilePath, mDeleteIfExists, clientOptions);
        DataOutputStream os = new DataOutputStream(fileOutStream);
        try {
            os.writeInt(mLength);
            for (int i = 0; i < mLength; i++) {
                os.writeInt(i);
            }
        } finally {
            os.close();
        }
    }

    private static FileOutStream getOrCreate(TachyonFileSystem tachyonFileSystem, TachyonURI filePath,
                                             boolean deleteIfExists, OutStreamOptions clientOptions) throws IOException, TachyonException {
        TachyonFile file;

        try {
            file = tachyonFileSystem.open(filePath);
        } catch (Exception e) {
            file = null;
        }
        if (file == null) {
            // file doesn't exist yet, so create it
            return tachyonFileSystem.getOutStream(filePath, clientOptions);
        } else if (deleteIfExists) {
            // file exists, so delete it and recreate
            tachyonFileSystem.delete(file);
            return tachyonFileSystem.getOutStream(filePath, clientOptions);
        }
        // file exists and deleteIfExists is false
        throw new FileAlreadyExistsException("File exists and deleteIfExists is false");
    }
}

