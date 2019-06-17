package net.bittx.hbase.spring.boot.starter;

import org.apache.hadoop.hbase.client.Table;

public interface TableCallback<T> {


    /**
     * Gets called by {@link HBaseTemplate} execute with an active Hbase table.
     * Does need to care about activating or closing down the table.
     *
     * @param table
     * @return
     * @throws Throwable
     */
    T doInTable(Table table) throws Throwable;
}
