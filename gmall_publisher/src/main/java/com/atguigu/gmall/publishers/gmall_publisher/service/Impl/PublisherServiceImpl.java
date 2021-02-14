package com.atguigu.gmall.publishers.gmall_publisher.service.Impl;

import com.atguigu.gmall.common.constant.GmallConstant;
import com.atguigu.gmall.publishers.gmall_publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
//import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    JestClient jestClient;

    @Override
    public Integer getDauTotal(String date) {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"bool\": {\n"
                + "      \"filter\": {\n"
                + "        \"term\": {\n"
                + "          \"logDate\": \"2021-02-14\"\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}";

        //SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        //boolQueryBuilder.filter(new TermQueryBuilder("logDate",date));
        //searchSourceBuilder.query(boolQueryBuilder);

        Search search = new Search.Builder(query).addIndex(GmallConstant.ES_INDEX_DAU).addType("_doc").build();
        Integer total = 0;
        try {
            SearchResult searchResult = jestClient.execute(search);
            total = searchResult.getTotal();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return total;
    }
}
