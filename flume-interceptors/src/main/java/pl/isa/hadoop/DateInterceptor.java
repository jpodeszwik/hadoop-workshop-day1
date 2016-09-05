package pl.isa.hadoop;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

public class DateInterceptor implements Interceptor {
        private Logger logger = Logger.getLogger(DateInterceptor.class);
        public void initialize() {
                logger.info("Starting DateInterceptor");
        }

        public Event intercept(Event event) {
                //163.109.86.19|-|-|2016-09-03 12:19:00|PUT /apps/cart.jsp?appID=4882 HTTP/1.0|404|4995|http://www.rowe.com/homepage.html|Mozilla/5.0 (X11; Linux i686; rv:1.9.6.20) Gecko/2013-11-07 07:18:44 Firefox/8.0
                String body = new String(event.getBody());
                String[] bodyParts = body.split("\\|");
                String dateString = bodyParts[3];
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                try {
                        long timestamp = sdf.parse(dateString).getTime();
                        event.getHeaders().put("timestamp", String.valueOf(timestamp));
                } catch (ParseException e) {
                        logger.error("Could not parse date " + dateString);
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

                public Interceptor build() {
                        return new DateInterceptor();
                }

                public void configure(Context context) {

                }
        }
}
