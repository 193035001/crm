package com.application;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.VersionType;
import co.elastic.clients.elasticsearch.core.CreateRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ESTest {


    public static void main(String[] args) throws Exception {

        RestClient restClient =RestClient.builder(new HttpHost("localhost",9200)).build();

        ElasticsearchTransport transport = new RestClientTransport(restClient,new JacksonJsonpMapper());

        ElasticsearchClient client = new ElasticsearchClient(transport);


        /**
         * 创建索引
         */
        String index="user-index";
        String aliases="user-aliases-001";
        client.indices().create(c -> c
                        .index(index)
                        .aliases(aliases, a -> a
                        .isWriteIndex(true)));

        /**
         * 创建数据文档
         */
        HashMap<String, String> map = new HashMap<>();
        map.put("username","张三");
        map.put("password","123456");
        CreateRequest<Object> dataStreamResponse=CreateRequest.of(e ->e
                .index(index)
                .id("1")
                .versionType(VersionType.valueOf("_doc"))
                .document(map));

        /**
         * 查询索引
         */
        List<Object> resultList = new ArrayList<>();

        SearchRequest searchRequest=SearchRequest.of(e ->e
                .index(index));

        SearchResponse<Object> response=client.search(searchRequest,Object.class);
        if (response.hits() !=null){
            List<Hit<Object>> list=response.hits().hits();

            for (Hit<Object> hit : list){
                Object t = (Object)hit.source();
                resultList.add(t);
            }
        }
        System.out.println(resultList);

        /**
         * 删除索引
         */
        client.delete(c ->c .index(index));

        client.delete(c -> c .index(index).id("1"));
    }
}
