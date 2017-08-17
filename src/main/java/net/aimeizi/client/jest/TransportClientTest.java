package net.aimeizi.client.jest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.aimeizi.Common.Contacts;
import net.aimeizi.Common.ObjectUtils;
import net.aimeizi.client.elasticsearch.EsManager;
import net.aimeizi.client.elasticsearch.Tools;
import net.aimeizi.model.Article;
import net.aimeizi.service.EsTransportService;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;


import java.io.IOException;
import java.util.Map;

public class TransportClientTest {

    //@Autowired
    //EsTransportService esTransportService;

    @Test
    public void search() throws IOException {
        String id = "112";
        EsManager.getInstance().init();
        GetResponse response = EsManager.getInstance().getClient().prepareGet(Contacts.INDEX, Contacts.TYPE, id)
                //.setOperationThreaded(false)
                .execute()
                .actionGet();
        boolean result = response.isExists();
        String json = response.getSourceAsString();
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);//注意：此设置可以使object中属性可以比json中key少
        Article o = mapper.readValue(json, Article.class);

        int i = 0;
    }
}
