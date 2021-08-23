package com.intel;

import com.intel.featureStorage.utils.Utils;
import jakarta.xml.bind.DatatypeConverter;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import com.intel.featureStorage.HbaseClient;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.List;
import java.util.Random;
import java.util.Vector;

public class WriteWorker implements Runnable {
    private String tn;
    private long returnValue = 0;
    private int recordPerTransaction;
    private List<byte[]> rks;
    private int pos;
    private HbaseClient hbaseClient = new HbaseClient();

    WriteWorker(String tn, int recordPerTransaction, List<byte[]> rks, int pos){
        this.tn = tn;
        this.recordPerTransaction = recordPerTransaction;
        this.rks = rks;
        this.pos = pos;
    }

    public long getReturnValue(){
        return returnValue;
    }

    @Override
    public void run() {
        byte[] vector = new byte[512];
        try {
            SecureRandom.getInstance("SHA1PRNG").nextBytes(vector);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        String b64 = DatatypeConverter.printBase64Binary(vector);

        try {
            List<Put> puts = new Vector<>();
            for (int i = 0; i < recordPerTransaction; i++) {
                Put put = new Put(rks.get(recordPerTransaction * pos + i));
                put.addColumn(Bytes.toBytes("t"), Bytes.toBytes("media_uri"),
                        Bytes.toBytes("uuiduuiduuiduuiduuiduuiduuiduuiduuiduuid"));
                put.addColumn(Bytes.toBytes("t"), Bytes.toBytes("roi_x"),
                        Utils.intToBytes(new Random().nextInt(100)));
                put.addColumn(Bytes.toBytes("t"), Bytes.toBytes("roi_y"),
                        Utils.intToBytes(new Random().nextInt(100)));
                put.addColumn(Bytes.toBytes("t"), Bytes.toBytes("roi_w"),
                        Utils.intToBytes(new Random().nextInt(100)));
                put.addColumn(Bytes.toBytes("t"), Bytes.toBytes("roi_h"),
                        Utils.intToBytes(new Random().nextInt(100)));
                put.addColumn(Bytes.toBytes("t"), Bytes.toBytes("feature_vector"),
                        Bytes.toBytes(b64));
                puts.add(put);
            }
            returnValue = hbaseClient.put(tn, puts);
        }catch (IOException ex) {
            ex.printStackTrace();
        }finally {
            if(hbaseClient != null) {
                try {
                    hbaseClient.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
