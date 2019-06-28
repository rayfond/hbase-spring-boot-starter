package net.bittx.hbase.service;


import net.bittx.hbase.spring.boot.starter.HBaseTemplate;
import net.bittx.hbase.spring.boot.starter.RowMapper;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

@Service
public class StarterService {

    @Resource
    HBaseTemplate hBaseTemplate;

    public HBaseTemplate gethBaseTemplate() {
        return hBaseTemplate;
    }

   public Object get(){
        return  hBaseTemplate.find("mytable", "cf", "col1", new RowMapper<String>() {
            @Override
            public String mapRow(Result result, int i) throws Exception {
                return Bytes.toString(result.value());
            }
        });
   }
}
