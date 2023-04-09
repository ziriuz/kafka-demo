package dev.siriuz.consumer.service;

import com.google.gson.JsonParser;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class OpenSearchService {

    private final Logger logger = LoggerFactory.getLogger(OpenSearchService.class);

    private BulkRequest bulkRequest = new BulkRequest();

    @Autowired
    private RestHighLevelClient openSearchClient;

    public void createIndex(String indexName) throws IOException {
        boolean indexExists = openSearchClient.indices().exists(
                new GetIndexRequest(indexName), RequestOptions.DEFAULT
        );

        if (!indexExists){
            openSearchClient.indices().create(
                    new CreateIndexRequest(indexName),
                    RequestOptions.DEFAULT
            );
            logger.info("OpenSearch index created: " + indexName);
        }
        else {
            logger.info("OpenSearch index already exists: " + indexName);
        }

    }

    public void insert(String index, String value, XContentType xContentType) {
        try {
            String id = extractId(value);
            if (id == null)
                logger.error("ID is null");

            logger.info("ID: " + id);
            IndexRequest indexRequest = new IndexRequest(index)
                    .source(value, xContentType)
                    .id(id);
            IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
            logger.info("Opensearch response status: " + response.status() + "; id: " + response.getId());
        } catch (IOException e) {
            logger.error("Opensearch insert IO error: ", e);
            //throw new RuntimeException(e);
        } catch (Exception e) {
            logger.error("Opensearch insert error: ", e);
        }
    }

    public void bulkInsert(String index, String value, XContentType xContentType) {
        String id = extractId(value);
        IndexRequest indexRequest = new IndexRequest(index)
                .source(value, xContentType)
                .id(id);
        bulkRequest.add(indexRequest);
    }

    public void executeBulkRequest() throws IOException {
        if (bulkRequest.numberOfActions() > 0){
            BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            logger.info("Bulk insert status: " + bulkResponse.status() +
                    "; inserted " + bulkResponse.getItems().length + " records");
            bulkRequest.requests().clear();
        }
    }



    private String extractId(String jsonString){
        return JsonParser.parseString(jsonString)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }
}
