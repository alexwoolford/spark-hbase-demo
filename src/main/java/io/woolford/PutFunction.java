package io.woolford;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;

import java.sql.Timestamp;


public class PutFunction implements Function<GenericRowWithSchema, Put> {

    private static final long serialVersionUID = 1L;

    @Override
    public Put call(GenericRowWithSchema genericRowWithSchema) throws Exception {

        int pk = genericRowWithSchema.getInt(0);
        double val = genericRowWithSchema.getDouble(1);
        Timestamp ts = genericRowWithSchema.getTimestamp(2);

        Put put = new Put(Bytes.toBytes(String.valueOf(pk)));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("val"), ts.getTime(), Bytes.toBytes(String.valueOf(val)));

        return put;
    }
}