package pl.isa.hadoop;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.jar.Pack200;

public class HostnameInterceptor implements Interceptor {
        private Logger logger = Logger.getLogger(HostnameInterceptor.class);
        private String hostnameKey;

        public HostnameInterceptor(String hostnameKey) {
                this.hostnameKey = hostnameKey;
        }

        public void initialize() {
                logger.info("Intializing with hostnameKey=" + hostnameKey);
        }

        public Event intercept(Event event) {
                try {
                        event.getHeaders().put("hostname", InetAddress.getLocalHost().getHostName());
                } catch (UnknownHostException e) {
                        logger.error("Unable to get hostname", e);
                }
                return event;
        }

        public List<Event> intercept(List<Event> list) {
                for(Event e : list) {
                        intercept(e);
                }
                return list;
        }

        public void close() {

        }

        public static class Builder implements Interceptor.Builder {
                private String hostnameKey;

                public Interceptor build() {
                        return new HostnameInterceptor(hostnameKey);
                }

                public void configure(Context context) {
                        hostnameKey = context.getString("header", "hostname");
                }
        }
}
