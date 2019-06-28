package net.bittx.hbase.service;

import net.bittx.hbase.spring.boot.starter.HBaseTemplate;
import net.bittx.hbase.spring.boot.starter.PutExt;
import net.bittx.hbase.spring.boot.starter.RowMapper;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

@Service
public class StarterService {

    private final String TABLE = "mytable";

    @Resource
    HBaseTemplate hBaseTemplate;

    public HBaseTemplate gethBaseTemplate() {
        return hBaseTemplate;
    }

   public Object get(){
        return  hBaseTemplate.find(TABLE, "cf", "col1", new RowMapper<String>() {
            @Override
            public String mapRow(Result result, int i) throws Exception {
                return Bytes.toString(result.value());
            }
        });
   }

   public void save(){
      hBaseTemplate.execute(TABLE,(t)->{
          PutExt pe = new PutExt("cf", "10001".getBytes())
                  .put("name", "myName")
                  .put("mem", "myMem");
          t.mutate(pe);
          return;
      });
   }
}
