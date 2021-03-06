package net.aimeizi.controller;

import net.aimeizi.Common.Contacts;
import net.aimeizi.client.elasticsearch.EsManager;
import net.aimeizi.client.elasticsearch.Tools;
import net.aimeizi.model.Article;
import net.aimeizi.service.ArticleService;
import net.aimeizi.service.EsTransportService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Controller
public class ArticleController {

    @Autowired
//	@Resource
    private ArticleService articleService;

    @Autowired
    private EsTransportService esTransportService;

    @RequestMapping(value = "/", method = RequestMethod.GET)
    public String tosearch() {
        return "search";
    }

    @RequestMapping(value = "/search", method = RequestMethod.POST)
    public ModelAndView search(HttpServletRequest request, HttpServletResponse response) {
        ModelAndView modelAndView = new ModelAndView();
        modelAndView.setViewName("search");
        String field = request.getParameter("field");
        String queryString = request.getParameter("queryString");
        String older = request.getParameter("older");
        String pageNumber = request.getParameter("pageNumber");
        String pageSize = request.getParameter("pageSize");
        if (StringUtils.isEmpty(queryString)) {
            return modelAndView;
        }
        try {
            if (StringUtils.isEmpty(pageNumber) || StringUtils.isEmpty(pageSize)) {
                pageNumber = String.valueOf("1");
                pageSize = String.valueOf("10");
            }

            //Map<String, Object> maps = esTransportService.search(Article.class, Contacts.INDEX, Contacts.TYPE, field, queryString, Integer.parseInt(pageNumber), Integer.parseInt(pageSize));
            //Map<String, Object> maps = esTransportService.search(Article.class, Contacts.INDEX, Contacts.TYPE, queryString, Integer.parseInt(pageNumber), Integer.parseInt(pageSize));

            Map<String, Object> maps = esTransportService.search(Article.class, Contacts.INDEX, Contacts.TYPE, "pubdate", "2017-08-15", null, Integer.parseInt(pageNumber), Integer.parseInt(pageSize));

            modelAndView.addObject("queryString", queryString);
            modelAndView.addObject("articles", (List<Article>) maps.get("articles"));
            Object count = maps.get("count");
            Object took = maps.get("took");
            modelAndView.addObject("count", count);
            modelAndView.addObject("took", took);
            modelAndView.addObject("field", field);
            modelAndView.addObject("pageNumber", pageNumber);
            modelAndView.addObject("pageSize", pageSize);
            long countL = (Long)count;
            long totalPage = countL % Integer.parseInt(pageSize) == 0 ? countL / Integer.parseInt(pageSize) : countL / Integer.parseInt(pageSize) + 1;
            modelAndView.addObject("totalPages", totalPage);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return modelAndView;
    }

}