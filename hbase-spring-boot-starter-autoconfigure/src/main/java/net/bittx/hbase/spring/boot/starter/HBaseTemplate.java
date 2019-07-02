package net.bittx.hbase.spring.boot.starter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.util.Assert;
import org.springframework.util.StopWatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Central class for accessing the HBase API. Simplifies the use of HBase and helps to avoid common errors.
 * It executes core HBase workflow, leaving application code to invoke actions and extract results.
 *
 * @author Costin Leau
 * @author Shaun Elliott
 */

public class HBaseTemplate implements HBaseOperations {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseOperations.class);

    private Configuration configuration;

    private volatile Connection connection;

    public HBaseTemplate(Configuration configuration) {
        this.configuration = configuration;
        Assert.notNull(configuration, " a valid configuration is required");
    }



    /**
     * Executes the given action against the specified table handling resource management.
     * <p>
     * Application exceptions thrown by the action object get propagated to the caller (can only be unchecked).
     * Allows for returning a result object (typically a domain object or collection of domain objects).
     *
     * @param tableName the target table
     * @param mapper
     * @return the result object of the callback action, or null
     */
    @Override
    public <T> T execute(String tableName, TableCallback<T> mapper) {
        Assert.notNull(mapper,"Callback object must not be null");
        Assert.notNull(tableName,"No table specified");

        StopWatch sw = new StopWatch();
        sw.start();

        Table table = null;


        try {
            table = getConnection().getTable(TableName.valueOf(tableName));
            return mapper.doInTable(table);
        }catch (Throwable throwable){
            throw new HBaseSystemException(throwable);
        }finally {
            if(null != table){
                try {
                    table.close();
                    sw.stop();
                }catch (IOException e){
                    LOGGER.error("HBase resource release failed.");
                }
            }
        }
    }

    /**
     * Scans the target table, using the given column family.
     * The content is processed row by row by the given action, returning a list of domain objects.
     *
     * @param tableName target table
     * @param family    column family
     * @param mapper
     * @return a list of objects mapping the scanned rows
     */
    @Override
    public <T> List<T> find(String tableName, String family, RowMapper<T> mapper) {
        Scan scan = new Scan();
        scan.setCaching(5000);
        scan.addFamily(Bytes.toBytes(family));
        return find(tableName,scan,mapper);
    }

    /**
     * Scans the target table, using the given column family.
     * The content is processed row by row by the given action, returning a list of domain objects.
     *
     * @param tableName target table
     * @param family    column family
     * @param qualifier column qualifier
     * @param mapper
     * @return a list of objects mapping the scanned rows
     */
    @Override
    public <T> List<T> find(String tableName, String family, String qualifier, RowMapper<T> mapper) {

        Scan scan = new Scan();
        scan.setCaching(5000);
        scan.addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifier));
        return find(tableName,scan,mapper);
    }

    /**
     * Scans the target table using the given {@link Scan} object. Suitable for maximum control over the scanning
     * process.
     * The content is processed row by row by the given action, returning a list of domain objects.
     *
     * @param tableName target table
     * @param scan      table scanner
     * @param mapper
     * @return a list of objects mapping the scanned rows
     */
    @Override
    public <T> List<T> find(String tableName, Scan scan, RowMapper<T> mapper) {
        return execute(tableName, new TableCallback<List<T>>() {
            @Override
            public List<T> doInTable(Table table) throws Throwable {
                // 一次读取回来的Results数量为100
                int caching = scan.getCaching();
                // if caching is default value (1), reset it to 5000
                if(caching == -1){
                    scan.setCaching(500);
                }

                ResultScanner scanner = table.getScanner(scan);

                try {
                    List<T> rs = new ArrayList<>();
                    int rowNum = 0;
                    for (Result r : scanner){
                        rs.add(mapper.mapRow(r,rowNum ++));
                    }
                    return rs;
                }finally {
                    scanner.close();
                }
            }
        });
    }

    /**
     * Gets an individual row from the given table. The content is mapped by the given action.
     *
     * @param tableName target table
     * @param rowName   row name
     * @param mapper    row mapper
     * @return object mapping the target row
     */
    @Override
    public <T> T get(String tableName, String rowName, RowMapper<T> mapper) {
        return get(tableName,rowName,null,null,mapper);
    }

    /**
     * Gets an individual row from the given table. The content is mapped by the given action.
     *
     * @param tableName  target table
     * @param rowName    row name
     * @param familyName column family
     * @param mapper     row mapper
     * @return object mapping the target row
     */
    @Override
    public <T> T get(String tableName, String rowName, String familyName, RowMapper<T> mapper) {
        return get(tableName,rowName,familyName,null,mapper);
    }

    /**
     * Gets an individual row from the given table. The content is mapped by the given action.
     *
     * @param tableName  target table
     * @param rowName    row name
     * @param familyName family
     * @param qualifier  column qualifier
     * @param mapper     row mapper
     * @return object mapping the target row
     */
    @Override
    public <T> T get(String tableName, String rowName, String familyName, String qualifier, RowMapper<T> mapper) {
        return execute(tableName, new TableCallback<T>() {
            @Override
            public T doInTable(Table table) throws Throwable {
                Get get = new Get(Bytes.toBytes(rowName));
                if(StringUtils.hasText(familyName)){
                    byte[] family = Bytes.toBytes(familyName);
                    if (StringUtils.hasText(qualifier)) {
                        get.addColumn(family, Bytes.toBytes(qualifier));
                    }
                    else{
                        get.addFamily(family);
                    }
                }
                Result result = table.get(get);
                return mapper.mapRow(result,0);
            }
        });
    }

    /**
     * 执行put update or delete
     *
     * @param tableName
     * @param action
     */
    @Override
    public void execute(String tableName, MutatorCallback action) {

        Assert.notNull(action, "Callback object must not be null");
        Assert.notNull(tableName, "No table specified");
        StopWatch sw = new StopWatch();
        sw.start();

        BufferedMutator mutator = null;

        try {
            BufferedMutatorParams mutatorParams = new BufferedMutatorParams(TableName.valueOf(tableName));
            mutator = getConnection().getBufferedMutator(mutatorParams);
            action.doInMutator(mutator);
        } catch (Throwable throwable) {
            sw.stop();
            throw new HBaseSystemException(throwable);
        }finally {

            if (null != mutator) {
                try {
                    mutator.flush();
                    mutator.close();
                    sw.stop();
                } catch (Exception e) {
                    LOGGER.error("hbase resource release failed.");
                }
            }

        }
    }

    /**
     * @param tableName
     * @param mutation
     */
    @Override
    public void saveOrUpdate(String tableName, Mutation mutation) {

        execute(tableName, new MutatorCallback() {
            @Override
            public void doInMutator(BufferedMutator mutator) throws Throwable {
                mutator.mutate(mutation);
            }
        });

    }

    /**
     * @param tableName
     * @param mutations
     */
    @Override
    public void saveOrUpdates(String tableName, List<Mutation> mutations) {
        execute(tableName, new MutatorCallback() {
            @Override
            public void doInMutator(BufferedMutator mutator) throws Throwable {
                mutator.mutate(mutations);
            }
        });
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public Connection getConnection() {
        if (null == this.connection) {
            synchronized (this) {
                if (null == this.connection) {
                    try {
                        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(2, Integer.MAX_VALUE,
                                60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
                        // init pool
                        poolExecutor.prestartCoreThread();
                        this.connection = ConnectionFactory.createConnection(configuration, poolExecutor);
                        //this.connection = ConnectionFactory.createConnection(configuration);
                    } catch (IOException e) {
                        LOGGER.error("Create hbase connection error: thread pool create failed.");
                    }
                }
            }
        }
        System.out.println(Thread.currentThread().getId());
        return this.connection;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

}
