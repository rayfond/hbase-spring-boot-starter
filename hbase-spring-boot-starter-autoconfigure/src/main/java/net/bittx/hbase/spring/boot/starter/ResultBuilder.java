package net.bittx.hbase.spring.boot.starter;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.util.StringUtils;

import java.beans.PropertyDescriptor;
import java.util.*;

/**
 *
 * @param <T>
 */
public class ResultBuilder<T> {
    private static final Logger log = LoggerFactory.getLogger(ResultBuilder.class);

    private Class<T> mappedClass;
    private Map<String, PropertyDescriptor> mappedFields;
    private Set<String> mappedProperties;
    HashSet populatedProperties;
    private BeanWrapper beanWrapper;
    private Result result;
    private String columnFamilyName;
    private T t;



    public ResultBuilder(String columnFamilyName, Result result, Class<T> clazz) {
        this.columnFamilyName = columnFamilyName;
        this.result = result;
        this.mappedClass = clazz;
        mappedFields = new HashMap<>();
        mappedProperties = new HashSet<>();
        populatedProperties = new HashSet<>();
        this.t = BeanUtils.instantiate(clazz);
        PropertyDescriptor[] pds = BeanUtils.getPropertyDescriptors(mappedClass);
        int len = pds.length;
        for (int i = 0; i < len; ++i) {
            PropertyDescriptor pd = pds[i];
            if (pd.getWriteMethod() != null) {
                this.mappedFields.put(this.lowerCaseName(pd.getName()), pd);
                String underscoredName = this.underscoreName(pd.getName());
                if (!this.lowerCaseName(pd.getName()).equals(underscoredName)) {
                    this.mappedFields.put(underscoredName, pd);
                }
                this.mappedProperties.add(pd.getName());
            }
        }
        beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(t);
    }


    private String underscoreName(String name) {
        if (!StringUtils.hasLength(name)) {
            return "";
        } else {
            StringBuilder result = new StringBuilder();
            result.append(this.lowerCaseName(name.substring(0, 1)));

            for (int i = 1; i < name.length(); ++i) {
                String s = name.substring(i, i + 1);
                String slc = this.lowerCaseName(s);
                if (!s.equals(slc)) {
                    result.append("_").append(slc);
                } else {
                    result.append(s);
                }
            }

            return result.toString();
        }
    }

    private String lowerCaseName(String name) {
        return name.toLowerCase(Locale.US);
    }
    //使用时根据要解析的字段频繁调用此方法即可，仿造java8 流式操作
    public ResultBuilder build(String columnName) {
        byte[] value = result.getValue(columnFamilyName.getBytes(), columnName.getBytes());
        if (value == null || value.length == 0) {
            return this;
        } else {
            String field = this.lowerCaseName(columnName.replaceAll(" ", ""));
            PropertyDescriptor pd = this.mappedFields.get(field);
            if (pd == null) {
                log.error("ResultBuilder error: can not find property: " + field);
            } else {
                beanWrapper.setPropertyValue(pd.getName(), Bytes.toString(value));
                populatedProperties.add(pd.getName());
            }
        }
        return this;
    }

    public T fetch() {
        if (populatedProperties.isEmpty()) {
            return null;
        }
        //有一个属性被解析就返回结果对象，hbase存的是稀疏数据，不一定全量
        return this.t;
    }
}
