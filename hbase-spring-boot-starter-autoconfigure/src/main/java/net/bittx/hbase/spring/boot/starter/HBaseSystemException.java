package net.bittx.hbase.spring.boot.starter;

import org.springframework.dao.UncategorizedDataAccessException;

/**
 * HBase Data Access exception.
 *
 * @author Costin Leau
 */
public class HBaseSystemException extends UncategorizedDataAccessException {

    public HBaseSystemException(Exception e){
        super(e.getMessage(), e);
    }

    public HBaseSystemException(Throwable cause) {
        super(cause.getMessage(), cause);
    }
}
