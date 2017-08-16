package net.aimeizi.client.jest;

import net.aimeizi.Common.Contacts;
import net.aimeizi.client.elasticsearch.EsManager;
import net.aimeizi.client.elasticsearch.Tools;
import net.aimeizi.model.Article;
import net.aimeizi.service.EsTransportService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;


import java.util.Map;

public class TransportClientTest {

    @Autowired
    EsTransportService esTransportService;

   // @Test
    public void search(){
        EsManager.getInstance().init();
       //List<Map<String, Object>> map =  Tools.querySearch(Contacts.INDEX, Contacts.TYPE, "author", "willa");
        Map<String, Object> map = null;

        try {
            map = esTransportService.search(Article.class, Contacts.INDEX, Contacts.TYPE, "author", "willa", 1, 10);
        } catch (Exception e) {
            e.printStackTrace();
        }

        EsManager.getInstance().destroy();
    }
}
