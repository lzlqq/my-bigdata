package com.leo.test;

import java.net.InetAddress;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

@SuppressWarnings("resource")
public class ESDemo {

	// 从es中查询数据
	@Test
	public void test1() throws Exception {
		// 指定ES集群
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		// 创建访问es服务器的客户端
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.182.101"), 9300));
		// 数据查询
		GetResponse response = client.prepareGet("index1", "blog", "0flc3W4BpqWuCN1usip6").execute().actionGet();
		// 得到查询出的数据
		System.out.println(response.getSourceAsString());
		client.close();

	}

	// 添加文档
	@Test
	public void test2() throws Exception {
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.182.101"), 9300));

		XContentBuilder doc = XContentFactory.jsonBuilder()
				.startObject()
				.field("id", "5")
				.field("title", "策略观察者模式")
				.field("content", "策略观察者简化代码")
				.field("postdate", "2015-02-03")
				.field("url", "csdn.net/79247746")
				.endObject();

		IndexResponse response = client.prepareIndex("index1", "blog", null).setSource(doc).get();

		System.out.println(response.status());
		client.close();
	}

	//删除文档
	@Test
	public void test3() throws Exception {
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.182.101"), 9300));

		DeleteResponse response = client.prepareDelete("index1", "blog", "8").get();

		System.out.println(response.status());
		client.close();
	}

	//更新文档
	@Test
	public void test4() throws Exception {
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.182.101"), 9300));

		UpdateRequest request = new UpdateRequest();
		request.index("index1")
				.type("blog")
				.id("0vld3W4BpqWuCN1u8irN")
				.doc(XContentFactory.jsonBuilder().startObject()
						.field("id", 4)
						.endObject());
			
		UpdateResponse response = client.update(request).get();
		System.out.println(response.status());
		client.close();
	}
	//upsert
	@Test
	public void test5() throws Exception {
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.182.101"), 9300));
		//添加文档
		XContentBuilder doc = XContentFactory.jsonBuilder()
				.startObject()
				.field("id", "8")
				.field("title", "策略88观察88者模式")
				.field("content", "策略88观察88者简化代码")
				.field("postdate", "2017-02-03")
				.field("url", "csdn.net/79247746")
				.endObject();

		IndexRequest index = new IndexRequest("index1", "blog", "8").source(doc);
		UpdateRequest update = new UpdateRequest("index1", "blog", "8");
		update.doc(XContentFactory.jsonBuilder().startObject()
						.field("title", "责任连模式")
						.field("content","责任连模式很难")
						.endObject()).upsert(index);
			
		UpdateResponse response = client.update(update).get();
		System.out.println(response.status());
		client.close();
	}
	
	//批量查询
	@Test
	public void test6() throws Exception {
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.182.101"), 9300));
		
		MultiGetResponse response = client.prepareMultiGet()
				.add("index1", "blog", "8")
				.add("index1", "blog", "8","1")
				.get();

		for(MultiGetItemResponse item:response) {
			GetResponse gr = item.getResponse();
			if(gr!=null && gr.isExists()) {
				System.out.println(gr.getSourceAsString());
			}
		}
		client.close();
	}

	// bulk批量操作
	@Test
	public void test7() throws Exception {
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.182.101"), 9300));
		
		BulkRequestBuilder	 bulkRequestBuilder= client.prepareBulk();
		
		//批量添加
		bulkRequestBuilder.add(client.prepareIndex("index1", "blog", "8").setSource(XContentFactory.jsonBuilder().startObject()
				.field("title","python")
				.field("price", 99)
				.endObject()));
		
		bulkRequestBuilder.add(client.prepareIndex("index1", "blog", "1").setSource(XContentFactory.jsonBuilder()
				.startObject()
				.field("title", "VR")
				.field("price",29)
				.endObject()));
		
		BulkResponse response = bulkRequestBuilder.get();
		System.out.println(response.status());
		if(response.hasFailures()) {
			System.out.println("失败了");
		}
		client.close();
	}

	// 查询删除
	@Test
	public void test8() throws Exception {
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.182.101"), 9300));
		
		BulkByScrollResponse response = DeleteByQueryAction.INSTANCE
				.newRequestBuilder(client)
				.filter(QueryBuilders.matchQuery("title", "工厂"))
				.source("index1")
				.get();
		
		long counts = response.getDeleted();
		System.out.println(counts);
	}

	// match_all
	@Test
	public void test9() throws Exception {
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.182.101"), 9300));
		QueryBuilder qb = QueryBuilders.matchAllQuery();
		SearchResponse sr = client.prepareSearch("index1")
					.setQuery(qb)
					.setSize(3)
					.get();
		SearchHits hits = sr.getHits();
		
		for (SearchHit searchHit : hits) {
			System.out.println(searchHit.getSourceAsString());
			Map<String, Object> map = searchHit.getSourceAsMap();
			for (String key : map.keySet()) {
				System.out.println(key+"="+map.get(key));
			}
		}
		client.close();
	}
	// match query
	@Test
	public void test10() throws Exception {
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.182.101"), 9300));
		QueryBuilder qb = QueryBuilders.matchQuery("interests", "changge");
		SearchResponse sr = client.prepareSearch("blog")
				.setQuery(qb)
				.setSize(3)
				.get();
		SearchHits hits = sr.getHits();
		for (SearchHit searchHit : hits) {
			System.out.println(searchHit.getSourceAsString());
			Map<String, Object> map = searchHit.getSourceAsMap();
			for (String key : map.keySet()) {
				System.out.println(key+"="+map.get(key));
			}
		}
		client.close();
	}

	// multi match query
	@Test
	public void test11() throws Exception {
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.182.101"), 9300));
		QueryBuilder qb = QueryBuilders.multiMatchQuery( "changge","interests","address");
		SearchResponse sr = client.prepareSearch("blog")
				.setQuery(qb)
				.setSize(3)
				.get();
		SearchHits hits = sr.getHits();
		for (SearchHit searchHit : hits) {
			System.out.println(searchHit.getSourceAsString());
			Map<String, Object> map = searchHit.getSourceAsMap();
			for (String key : map.keySet()) {
				System.out.println(key+"="+map.get(key));
			}
		}
		client.close();
	}

	// term
	@Test
	public void test12() throws Exception {
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.182.101"), 9300));
		QueryBuilder qb = QueryBuilders.termQuery("interests", "changge");
		SearchResponse sr = client.prepareSearch("blog")
				.setQuery(qb)
				.setSize(2)
				.get();
		SearchHits hits = sr.getHits();
		for (SearchHit searchHit : hits) {
			System.out.println(searchHit.getSourceAsString());
			Map<String, Object> map = searchHit.getSourceAsMap();
			for (String key : map.keySet()) {
				System.out.println(key+"="+map.get(key));
			}
		}
		client.close();
	}
	
	// terms 
	@Test
	public void test13() throws Exception {
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.182.101"), 9300));
		QueryBuilder qb = QueryBuilders.termsQuery("interests", "changge","lvyou");
		SearchResponse sr = client.prepareSearch("blog")
				.setQuery(qb)
				.setSize(2)
				.get();
		SearchHits hits = sr.getHits();
		for (SearchHit searchHit : hits) {
			System.out.println(searchHit.getSourceAsString());
			Map<String, Object> map = searchHit.getSourceAsMap();
			for (String key : map.keySet()) {
				System.out.println(key+"="+map.get(key));
			}
		}
		client.close();
	}
	@Test
	public void test14() throws Exception {
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.182.101"), 9300));
		// range查询
		//QueryBuilder qb = QueryBuilders.rangeQuery("birthday").from("1990-01-01").to("2000-12-31");
		// prefix查询
		//QueryBuilder qb = QueryBuilders.prefixQuery("name","zhao");
		//wildcard查询
		//QueryBuilder qb = QueryBuilders.wildcardQuery("name","zhao*");
		//fuzzy查询
		//QueryBuilder qb = QueryBuilders.fuzzyQuery("name","za");
		//ids查询
		QueryBuilder qb = QueryBuilders.idsQuery().addIds("1","2");
		SearchResponse sr = client.prepareSearch("blog")
				.setQuery(qb)
				.setSize(2)
				.get();
		SearchHits hits = sr.getHits();
		for (SearchHit searchHit : hits) {
			System.out.println(searchHit.getSourceAsString());
			Map<String, Object> map = searchHit.getSourceAsMap();
			for (String key : map.keySet()) {
				System.out.println(key+"="+map.get(key));
			}
		}
		client.close();
	}
	
	// 聚合查询 计数、最大值、最小值、平均值，综合
	@Test
	public void test15() throws Exception {
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.182.101"), 9300));
		AggregationBuilder agg = AggregationBuilders.max("aggMax").field("age");
		SearchResponse response = client.prepareSearch("index1").addAggregation(agg).get();
		Max max = response.getAggregations().get("aggMax");
		System.out.println(max.getValue());
		client.close();
	}
	
	// query string
	@Test
	public void test16() throws Exception {
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.182.101"), 9300));
		QueryBuilder qb = QueryBuilders.commonTermsQuery("interests", "changge");
		SearchResponse sr = client.prepareSearch("blog")
				.setQuery(qb)
				.setSize(2)
				.get();
		SearchHits hits = sr.getHits();
		for (SearchHit searchHit : hits) {
			System.out.println(searchHit.getSourceAsString());
			Map<String, Object> map = searchHit.getSourceAsMap();
			for (String key : map.keySet()) {
				System.out.println(key+"="+map.get(key));
			}
		}
		client.close();
	}
	@Test
	public void test17() throws Exception {
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.182.101"), 9300));
		// 包含changge，不包含hejiu,可以不同时满足
		// QueryBuilder qb = QueryBuilders.queryStringQuery("+changge -hejiu");
		// 同时满足
		QueryBuilder qb = QueryBuilders.simpleQueryStringQuery("+changge -hejiu");
		SearchResponse sr = client.prepareSearch("blog")
				.setQuery(qb)
				.setSize(2)
				.get();
		SearchHits hits = sr.getHits();
		for (SearchHit searchHit : hits) {
			System.out.println(searchHit.getSourceAsString());
			Map<String, Object> map = searchHit.getSourceAsMap();
			for (String key : map.keySet()) {
				System.out.println(key+"="+map.get(key));
			}
		}
		client.close();
	}
	
	// 组合查询
	@Test
	public void test18() throws Exception {
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.182.101"), 9300));
		QueryBuilder qb = QueryBuilders.boolQuery()
					.must(QueryBuilders.matchQuery("interests", "changge"))
					.mustNot(QueryBuilders.matchQuery("interests", "hejiu"))
					.should(QueryBuilders.matchQuery("address", "bei jing"))
					.filter(QueryBuilders.rangeQuery("birthday").gte("1999-01-01").format("yyyy-MM-dd"));
		SearchResponse sr = client.prepareSearch("blog")
				.setQuery(qb)
				.setSize(2)
				.get();
		SearchHits hits = sr.getHits();
		for (SearchHit searchHit : hits) {
			System.out.println(searchHit.getSourceAsString());
			Map<String, Object> map = searchHit.getSourceAsMap();
			for (String key : map.keySet()) {
				System.out.println(key+"="+map.get(key));
			}
		}
		client.close();
	}
	
	// 分组聚合
	@Test
	public void test19() throws Exception {
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.182.101"), 9300));
		AggregationBuilder agg = AggregationBuilders.terms("terms").field("age");
		SearchResponse response= client.prepareSearch("index1").addAggregation(agg).execute().actionGet();
		Terms terms = response.getAggregations().get("terms");
		for (Terms.Bucket entry : terms.getBuckets()) {
			System.out.println(entry.getKey()+":"+entry.getDocCount());
		}
		client.close();
	}
	
	// 分组过滤
	@Test
	public void test20() throws Exception {
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.182.101"), 9300));
		QueryBuilder qb = QueryBuilders.termQuery("age", 20);
		AggregationBuilder agg = AggregationBuilders.filter("filter", qb);
		SearchResponse response= client.prepareSearch("index1").addAggregation(agg).execute().actionGet();
		Filter filter = response.getAggregations().get("filter");
		System.out.println(filter.getDocCount());
		client.close();
	}
	@Test
	public void test21() throws Exception {
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.182.101"), 9300));
		
		AggregationBuilder agg = AggregationBuilders.filters("filters", 
				new FiltersAggregator.KeyedFilter("changge", QueryBuilders.termQuery("interests", "changge")));
		SearchResponse response= client.prepareSearch("index1").addAggregation(agg).execute().actionGet();
		Filters filters = response.getAggregations().get("filters");
		for (Filters.Bucket entry : filters.getBuckets()) {
			System.out.println(entry.getKey()+":"+entry.getDocCount());
		}
		client.close();
	}
	//range聚合
	@Test
	public void test22() throws Exception {
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.182.101"), 9300));
		AggregationBuilder agg = AggregationBuilders
				.range("range")
				.field("age")
				.addUnboundedTo(50)//(,to)
				.addRange(25, 50)//[from,to)
				.addUnboundedFrom(25);//[from,)
		SearchResponse response= client.prepareSearch("index1").addAggregation(agg).execute().actionGet();
		Range range = response.getAggregations().get("range");
		for (Range.Bucket entry : range.getBuckets()) {
			System.out.println(entry.getKey()+":"+entry.getDocCount());
		}
		client.close();
	}
	
	// missing
	@Test
	public void test23() throws Exception {
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.182.101"), 9300));
		AggregationBuilder agg = AggregationBuilders.missing("missing").field("price");
		SearchResponse response= client.prepareSearch("index1").addAggregation(agg).execute().actionGet();
		Aggregation aggregation=response.getAggregations().get("missing");
		System.out.println(aggregation.toString());
		client.close();
	}
}
