package pl.isa.hadoop;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

public class HostnameInterceptor implements Interceptor {
        private Logger logger = Logger.getLogger(HostnameInterceptor.class);

        public void initialize() {
                logger.info("Starting hostname interceptor");
        }

        public Event intercept(Event event) {
                try {
                        event.getHeaders().put("hostname", InetAddress.getLocalHost().getHostName());
                } catch (UnknownHostException e) {
                        logger.error("Unable to read hostname");
                }
                return event;
        }

        public List<Event> intercept(List<Event> list) {
                for(Event event : list) {
                        intercept(event);
                }
                return list;
        }

        public void close() {

        }

        public static class Builder implements Interceptor.Builder {

                public Interceptor build() {
                        return new HostnameInterceptor();
                }

                public void configure(Context context) {

                }
        }
}
