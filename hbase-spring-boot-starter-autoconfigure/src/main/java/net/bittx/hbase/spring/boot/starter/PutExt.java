package net.bittx.hbase.spring.boot.starter;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class PutExt extends Put {

    String family = "cf";

    public PutExt(String family, byte[] row) {
        super(row);
        this.family = family;
    }

    public PutExt put(String paramName,Object param) throws IOException{
        if(param != null){
            addColumn(family.getBytes(), paramName.getBytes(), Bytes.toBytes(param.toString()));
        }
        return this;
    }
}
