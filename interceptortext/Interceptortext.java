package interceptortext;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Interceptortext implements Interceptor{

    //声明一个存放事件的集合
    private List<Event> addHeaderEvents;

    @Override
    public void initialize() {

        //初始化
        addHeaderEvents = new ArrayList<>();
    }

    //单个事件拦截
    @Override
    public Event intercept(Event event) {

        //获取header
        Map<String, String> headers = event.getHeaders();

        //获取事件中的body
        String body = new String((event.getBody()));

        //根据body中是否有“hello”来决定添加怎样的头信息
        if (body.contains("hello")){

            headers.put("type","dicon");

        }else{
            headers.put("type","dyc");
        }

        return event;
    }

    //批量事件拦截
    @Override
    public List<Event> intercept(List<Event> list) {

        //清空集合
        addHeaderEvents.clear();

        //遍历集合给每一个events添加头信息
        for (Event event:list){
            addHeaderEvents.add(intercept(event));
        }

        return addHeaderEvents;
    }

    @Override
    public void close() {

    }

    public  static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new Interceptortext();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
