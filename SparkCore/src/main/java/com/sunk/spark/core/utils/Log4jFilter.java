package com.sunk.spark.core.utils;

import org.apache.log4j.Level;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;

/**
 * @author sunk
 * @since 2023/2/23
 */
public class Log4jFilter extends Filter {

    @Override
    public int decide(LoggingEvent event) {
        final String message = event.getMessage().toString();
        final Level level = event.getLevel();

        if (level.toString().equals("INFO")) {
            if (message.contains("Bound SparkUI")) {
                return Filter.ACCEPT;
            } else {
                return Filter.DENY;
            }
        }

        return Filter.ACCEPT;
    }

}
