package com.wf.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;
import org.graphstream.graph.implementations.SingleGraph;
import org.graphstream.graph.implementations.SingleNode;
import org.graphstream.ui.swingViewer.View;
import org.graphstream.ui.swingViewer.Viewer;
import org.json.JSONArray;
import org.json.JSONObject;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class Actor {
    public static void main(String[] args) throws IOException {
        //读取整个文件获取所有电影的前两个明星
        String path = "/data3.json";
        File file = new File(Actor.class.getResource(path).getFile());
        BufferedReader br = new BufferedReader(new FileReader(file));
        String s = null;
        HashSet<String> actorSet = new HashSet<String>();//存储所有演员名
        while ((s = br.readLine()) != null) {
            JSONObject jsonObject = new JSONObject(s);
            JSONArray actors = jsonObject.getJSONArray("actors");
            //System.out.println("--------------" + actors.toString());
            int actorLength = actors.length();
            if(actorLength==0)
                continue;
            else if(actorLength==1){
                //actorSet.add(actors.toString());
                continue;
            }else{
                //取前两个演员名
                actorSet.add(actors.get(0).toString());
                actorSet.add(actors.get(1).toString());
            }
        }
        br.close();
        //构建图的顶点
        List<Tuple2<Object,User>> vlist = new ArrayList<Tuple2<Object,User>>();
        long totalVertexNum = 1;
        for (String actor:actorSet) {
            vlist.add(new Tuple2<Object, User>(totalVertexNum,new User(actor)));
            //System.out.println("------vertex:"+vlist.get((int)totalVertexNum-1).productElement(0));
            //System.out.println("------name:"+((User)vlist.get((int)totalVertexNum-1).productElement(1)).getName());
            totalVertexNum ++;
        }
        //构建图的边
        List<Edge<String>> elist = new ArrayList<Edge<String>>();
        long srcId = 0;
        long dstId = 0;
        BufferedReader br2 = new BufferedReader(new FileReader(file));
        String s2 = null;
        while ((s2 = br2.readLine()) != null) {
            JSONObject jsonObject = new JSONObject(s2);
            JSONArray actors = jsonObject.getJSONArray("actors");
            String title = jsonObject.getString("title");
            //获取演员对应顶点的id
            int actorLength = actors.length();
            if(actorLength==0)
                continue;
            else if(actorLength==1){
                continue;
            }else{
                //取前两个演员名
                String actor1 = actors.get(0).toString();
                String actor2 = actors.get(1).toString();
                for (Tuple2 tuple:vlist) {
                    if(actor1.equals(((User)tuple.productElement(1)).getName())){
                        srcId = Long.parseLong(tuple.productElement(0).toString());
                        System.out.println("----------serId:"+srcId);
                        System.out.println("----------name:"+((User)tuple.productElement(1)).getName());
                    }
                    if(actor2.equals(((User)tuple.productElement(1)).getName())){
                        dstId = Long.parseLong(tuple.productElement(0).toString());
                        System.out.println("-----------dstId:"+dstId);
                        System.out.println("-----------name:"+((User)tuple.productElement(1)).getName());
                    }
                }
                System.out.println("--------------------title:"+title);
            }
            elist.add(new Edge<String>(srcId,dstId,title));
        }
        br.close();

        //构建图
        SparkConf conf = new SparkConf().setAppName("Actors").setMaster("local[2]");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        JavaRDD<Tuple2<Object,User>> vertexRdd = ctx.parallelize(vlist);
        JavaRDD<Edge<String>> edgeRdd = ctx.parallelize(elist);

        User defaultUser = new User("defaultUser");
        Graph<User,String> srcGraph = Graph.apply(vertexRdd.rdd(),edgeRdd.rdd(),defaultUser, StorageLevel.MEMORY_AND_DISK(),
                StorageLevel.MEMORY_AND_DISK(), ClassTag$.MODULE$.apply(User.class),ClassTag$.MODULE$.apply(String.class));

        System.out.println("---------The number of Edges:  " + srcGraph.ops().numEdges());
        System.out.println("---------The number of vertices:  "+ srcGraph.ops().numVertices());

        //可视化
        SingleGraph graphStream = new SingleGraph("GraphStream");
        graphStream.addAttribute("ui.stylesheet", "url(src/main/style/stylesheet.css)");
        graphStream.addAttribute("ui.quality");
        graphStream.addAttribute("ui.antialias");
        //加载顶点到GraphStream中
        List<Tuple2<Object,User>> verList = srcGraph.vertices().toJavaRDD().collect();
        for (Tuple2 tuple:verList) {
            String vid = tuple.productElement(0).toString();
            String name = ((User)tuple.productElement(1)).getName();
            SingleNode node = graphStream.addNode(vid);
            if(vid == "1484"){
                node.addAttribute("ui.class","liudehua");
                node.addAttribute("ui.label",vid+"\n"+name);
            }else{
                node.addAttribute("ui.label",vid+"\n"+name);
            }
        }
        //加载边到GraphStream中
        List<Edge<String>> edList = srcGraph.edges().toJavaRDD().collect();
        for (Edge edge:edList) {
            String sid = new Long(edge.srcId()).toString();
            String did = new Long(edge.dstId()).toString();
            String movieName = edge.attr().toString();
            graphStream.addEdge(movieName,sid,did,true).addAttribute("ui.label",movieName);
        }

        Viewer viewer = graphStream.display();
        View view = viewer.getDefaultView();
        view.resizeFrame(800,600);

    }
}
