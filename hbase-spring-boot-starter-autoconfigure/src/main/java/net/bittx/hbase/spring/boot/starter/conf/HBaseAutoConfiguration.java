package net.bittx.hbase.spring.boot.starter.conf;


import net.bittx.hbase.spring.boot.starter.HBaseTemplate;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableAutoConfiguration
@EnableConfigurationProperties(HBaseProperties.class)
@ConditionalOnClass(HBaseTemplate.class)
public class HBaseAutoConfiguration {
    private static final String HBASE_QUORUM = "hbase.zookeeper.quorum";
    private static final String HBASE_ROOTDIR = "hbase.root.dir";
    private static final String HBASE_ZNODE_PARENT = "zookeeper.znode.parent";


    @Autowired
    private HBaseProperties hBaseProperties;

    @Bean
    @ConditionalOnMissingBean(HBaseTemplate.class)
    public HBaseTemplate hBaseTemplate(){
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set(HBASE_QUORUM,hBaseProperties.getQuorum());
        conf.set(HBASE_ROOTDIR,hBaseProperties.getRootDir());
        conf.set(HBASE_ZNODE_PARENT,hBaseProperties.getNodeParent());
        return new HBaseTemplate(conf);
    }

}
