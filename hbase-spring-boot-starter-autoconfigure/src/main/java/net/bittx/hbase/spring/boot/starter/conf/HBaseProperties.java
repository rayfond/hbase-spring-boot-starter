package net.bittx.hbase.spring.boot.starter.conf;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("prefix=data.hbase")
public class HBaseProperties {

    private String quorum;
    private String rootDir;
    private String nodeParent;

    public String getQuorum() {
        return quorum;
    }

    public void setQuorum(String quorum) {
        this.quorum = quorum;
    }

    public String getRootDir() {
        return rootDir;
    }

    public void setRootDir(String rootDir) {
        this.rootDir = rootDir;
    }

    public String getNodeParent() {
        return nodeParent;
    }

    public void setNodeParent(String nodeParent) {
        this.nodeParent = nodeParent;
    }
}
