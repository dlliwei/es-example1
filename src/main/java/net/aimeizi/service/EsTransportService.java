package net.aimeizi.service;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;
import java.util.Map;

/**
 * Created by Administrator on 2015/9/21.
 */
public interface EsTransportService {

    public<T> int index(String index, String type, T object) throws JsonProcessingException;
    public<T> T getById(String _id, Class<T> classz) throws IOException;
    public<T> int update(String _id, T object);
    /**
     * 根据field和queryString全文检索
     * @param field
     * @param queryString
     * @param pageNumber
     * @param pageSize
     * @return
     */
    public<T> Map<String, Object> search(Class<T> classz, String index, String type, String field, String queryString, int pageNumber, int pageSize)  throws Exception;
    //public<T> Map<String, Object> search(Class<T> classz, String index, String type, Map<String, Object> searchMap, int pageNumber, int pageSize)  throws Exception;
}
