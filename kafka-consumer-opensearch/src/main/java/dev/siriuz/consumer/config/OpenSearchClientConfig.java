package dev.siriuz.consumer.config;

import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import java.net.URI;

@Configuration
public class OpenSearchClientConfig {

    @Value("${opensearch.connection}")
    private String connString;

    @Bean
    public RestHighLevelClient openSearchClient(){
        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost(connUri.getHost(), connUri.getPort(), "http")
                    )
            );
        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme())
                    )
                    .setHttpClientConfigCallback(
                            httpAsyncClientBuilder ->
                                    httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
                    )
            );
        }
        return restHighLevelClient;
    }
}
