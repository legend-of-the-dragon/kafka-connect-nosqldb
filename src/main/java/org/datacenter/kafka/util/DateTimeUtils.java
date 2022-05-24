package org.datacenter.kafka.util;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.chrono.ChronoZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author sky
 * @date 2022-05-10
 */
public class DateTimeUtils {

    static final long MILLISECONDS_PER_SECOND = TimeUnit.SECONDS.toMillis(1);
    static final long NANOSECONDS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);
    static final long NANOSECONDS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);
    static final DateTimeFormatter ISO_DATE_TIME_NANOS_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS");

    private static final ThreadLocal<Map<TimeZone, Calendar>> TIMEZONE_CALENDARS =
            ThreadLocal.withInitial(HashMap::new);

    private static final ThreadLocal<Map<TimeZone, SimpleDateFormat>> TIMEZONE_DATE_FORMATS =
            ThreadLocal.withInitial(HashMap::new);

    private static final ThreadLocal<Map<TimeZone, SimpleDateFormat>> TIMEZONE_TIME_FORMATS =
            ThreadLocal.withInitial(HashMap::new);

    private static final ThreadLocal<Map<TimeZone, SimpleDateFormat>> TIMEZONE_TIMESTAMP_FORMATS =
            ThreadLocal.withInitial(HashMap::new);

    public static Calendar getTimeZoneCalendar(final TimeZone timeZone) {
        return TIMEZONE_CALENDARS.get().computeIfAbsent(timeZone, GregorianCalendar::new);
    }

    public static String formatDate(Date date, TimeZone timeZone) {
        return TIMEZONE_DATE_FORMATS
                .get()
                .computeIfAbsent(
                        timeZone,
                        aTimeZone -> {
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                            sdf.setTimeZone(aTimeZone);
                            return sdf;
                        })
                .format(date);
    }

    public static String formatTime(Date date, TimeZone timeZone) {
        return TIMEZONE_TIME_FORMATS
                .get()
                .computeIfAbsent(
                        timeZone,
                        aTimeZone -> {
                            SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
                            sdf.setTimeZone(aTimeZone);
                            return sdf;
                        })
                .format(date);
    }

    public static String formatTimestamp(Date date, TimeZone timeZone) {
        return TIMEZONE_TIMESTAMP_FORMATS
                .get()
                .computeIfAbsent(
                        timeZone,
                        aTimeZone -> {
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                            sdf.setTimeZone(aTimeZone);
                            return sdf;
                        })
                .format(date);
    }

    private static Long convertToEpochNanos(Timestamp t) {
        Long epochMillis = TimeUnit.SECONDS.toNanos(t.getTime() / MILLISECONDS_PER_SECOND);
        Long nanosInSecond = TimeUnit.NANOSECONDS.toNanos(t.getNanos());
        return epochMillis + nanosInSecond;
    }

    /**
     * Get the number of nanoseconds past epoch of the given {@link Timestamp}.
     *
     * @param timestamp the Java timestamp value
     * @return the epoch nanoseconds
     */
    public static Long toEpochNanos(Timestamp timestamp) {
        return Optional.ofNullable(timestamp).map(DateTimeUtils::convertToEpochNanos).orElse(null);
    }

    /**
     * Get the number of nanoseconds past epoch of the given {@link Timestamp}.
     *
     * @param timestamp the Java timestamp value
     * @return the epoch nanoseconds string
     */
    public static String toEpochNanosString(Timestamp timestamp) {
        return Optional.ofNullable(timestamp)
                .map(DateTimeUtils::convertToEpochNanos)
                .map(String::valueOf)
                .orElse(null);
    }

    /**
     * Get the iso date-time string with nano precision for the given {@link Timestamp}.
     *
     * @param timestamp the Java timestamp value
     * @param tz the timezone of the source database
     * @return the string iso date time
     */
    public static String toIsoDateTimeString(Timestamp timestamp, TimeZone tz) {
        return Optional.ofNullable(timestamp)
                .map(Timestamp::toInstant)
                .map(t -> t.atZone(tz.toZoneId()))
                .map(t -> t.format(ISO_DATE_TIME_NANOS_FORMAT))
                .orElse(null);
    }

    /**
     * Get {@link Timestamp} from epoch with nano precision
     *
     * @param nanos epoch nanos in long
     * @return the equivalent java sql Timestamp
     */
    public static Timestamp toTimestamp(Long nanos) {
        return Optional.ofNullable(nanos)
                .map(
                        n -> {
                            Timestamp ts = new Timestamp(nanos / NANOSECONDS_PER_MILLISECOND);
                            ts.setNanos((int) (nanos % NANOSECONDS_PER_SECOND));
                            return ts;
                        })
                .orElse(null);
    }

    /**
     * Get {@link Timestamp} from epoch with nano precision
     *
     * @param nanos epoch nanos in string
     * @return the equivalent java sql Timestamp
     */
    public static Timestamp toTimestamp(String nanos) throws NumberFormatException {
        return Optional.ofNullable(nanos)
                .map(Long::parseLong)
                .map(DateTimeUtils::toTimestamp)
                .orElse(null);
    }

    /**
     * Get {@link Timestamp} from epoch with nano precision
     *
     * @param isoDT iso dateTime format "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS"
     * @param tz the timezone of the source database
     * @return the equivalent java sql Timestamp
     */
    public static Timestamp toTimestampFromIsoDateTime(String isoDT, TimeZone tz) {
        return Optional.ofNullable(isoDT)
                .map(i -> LocalDateTime.parse(isoDT, ISO_DATE_TIME_NANOS_FORMAT))
                .map(t -> t.atZone(tz.toZoneId()))
                .map(ChronoZonedDateTime::toInstant)
                .map(Timestamp::from)
                .orElse(null);
    }
}
