package net.bittx.hbase.service;

import net.bittx.hbase.spring.boot.starter.HBaseTemplate;
import net.bittx.hbase.spring.boot.starter.PutExt;
import net.bittx.hbase.spring.boot.starter.ResultBuilder;
import net.bittx.hbase.spring.boot.starter.RowMapper;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

@Service
public class StarterService {

    private final String TABLE = "mytable";
    private final String CF = "cf";

    @Resource
    HBaseTemplate hBaseTemplate;

    public HBaseTemplate gethBaseTemplate() {
        return hBaseTemplate;
    }

    public Object get() {
        return hBaseTemplate.find(TABLE, "cf", "col1", new RowMapper<String>() {
            @Override
            public String mapRow(Result result, int i) throws Exception {
                return Bytes.toString(result.value());
            }
        });
    }


    public Object get(String qualifier) {
        return hBaseTemplate.find(TABLE, "cf", qualifier, new RowMapper<String>() {
            @Override
            public String mapRow(Result result, int i) throws Exception {
                return Bytes.toString(result.value());
            }
        });
    }

    public void save() {
        hBaseTemplate.execute(TABLE, (t) -> {
            PutExt pe = new PutExt("cf", "13000".getBytes())
                    .put("name", "myName13000")
                    .put("mem", "myMem13000");
            t.mutate(pe);
            return;
        });
    }


    public UserInfo getUserInfo() {
        return (UserInfo) hBaseTemplate.get(TABLE, "10001", CF,
                (result, i) -> new ResultBuilder<>(CF, result, UserInfo.class)
                        .build("name")
                        .build("mem")
                        .fetch());
    }


    public static class UserInfo {
        String name;
        String mem;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getMem() {
            return mem;
        }

        public void setMem(String mem) {
            this.mem = mem;
        }
    }

    /**
     * GE min and LT max   [min, max)
     *
     * @return
     */
    public List<UserInfo> listUserInfo() {

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("11000"));
        scan.setStopRow(Bytes.toBytes("14000"));

        // All column
        scan.addFamily(CF.getBytes());
        // specified columns
        //scan.addColumn(CF.getBytes(),"name".getBytes());
        //scan.addColumn(CF.getBytes(),"mem".getBytes());

        return hBaseTemplate.find(TABLE, scan, new RowMapper<UserInfo>() {
                    @Override
                    public UserInfo mapRow(Result result, int rowNum) throws Exception {
                        return (UserInfo) new ResultBuilder<UserInfo>(CF, result, UserInfo.class).build("name").build("mem").fetch();
                    }
                }
        );

    }

}
