package com.leo.test;

import java.net.InetAddress;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
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

	@Test
	public void test6() throws Exception {

	}

	@Test
	public void test7() throws Exception {

	}

	@Test
	public void test8() throws Exception {

	}

	@Test
	public void test9() throws Exception {

	}

	@Test
	public void test10() throws Exception {

	}

	@Test
	public void test11() throws Exception {

	}

	@Test
	public void test12() throws Exception {

	}

}
