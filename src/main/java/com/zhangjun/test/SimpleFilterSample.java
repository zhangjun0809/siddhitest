
package com.zhangjun.test;

import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

import java.util.Arrays;

/**
 * The sample demonstrate how to use Siddhi within another Java program.
 * This sample contains a simple filter query.
 */
public class SimpleFilterSample {

    public static void main(String[] args) throws InterruptedException {

        // Creating Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                "define stream cseEventStream (symbol string, price float, volume long); " +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream[volume < 150] " +
                "select symbol,price,volume " +
                "insert into hello ;";

        // Generating runtime
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        // Adding callback to retrieve output events from query
        siddhiAppRuntime.addCallback("query1", new QueryCallback()
        {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents)
            {
                // EventPrinter.print(timeStamp, inEvents, removeEvents);
                System.out.println("inEvents"+Arrays.toString(inEvents));
//                inEvents[Event{timestamp=1592018594622, data=[Welcome, 700.0, 100], isExpired=false}]
//                inEvents[Event{timestamp=1592018594624, data=[to, 50.0, 30], isExpired=false}]
//                inEvents[Event{timestamp=1592018596625, data=[siddhi!, 45.6, 50], isExpired=false}]
                System.out.print(inEvents[0].getData(0) + " ");
                System.out.print(inEvents[0].isExpired()+ " ");

            }
        });

        // Retrieving InputHandler to push events into Siddhi
        //检索InputHandler以将事件推入Siddhi
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");

        // Starting event processing
        //开始事件的处理
        siddhiAppRuntime.start();

        // Sending events to Siddhi
        //向siddhi发送事件
        inputHandler.send(new Object[]{"Welcome", 700f, 100L});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 200L});
        inputHandler.send(new Object[]{"to", 50f, 30L});
        Thread.sleep(2000);
        inputHandler.send(new Object[]{"IBM", 76.6f, 400L});
        inputHandler.send(new Object[]{"siddhi!", 45.6f, 50L});
        Thread.sleep(500);

        // Shutting down the runtime
        siddhiAppRuntime.shutdown();

        // Shutting down Siddhi
        siddhiManager.shutdown();
    }
}