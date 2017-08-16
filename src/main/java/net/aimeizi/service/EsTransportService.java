package net.aimeizi.service;

import java.util.Map;

/**
 * Created by Administrator on 2015/9/21.
 */
public interface EsTransportService {

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
