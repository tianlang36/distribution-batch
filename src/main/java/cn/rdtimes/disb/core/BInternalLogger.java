package cn.rdtimes.disb.core;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 内部日志类
 * Created by BZ.
 */
public class BInternalLogger {
    public static Level logLevel = Level.DEBUG;

    public static void error(String msg) {
        error(msg, null);
    }

    public static void error(Class<?> clazz, String msg) {
        error(clazz, msg, null);
    }

    public static void error(String msg, Throwable cause) {
        error(null, msg, cause);
    }

    public static void error(Class<?> clazz, String msg, Throwable cause) {
        if (logLevel.ordinal() <= Level.ERROR.ordinal()) {
            systemPrintln(clazz, "ERROR", msg, cause);
        }
    }

    public static void warning(String msg) {
        warning(msg, null);
    }

    public static void warning(Class<?> clazz, String msg) {
        warning(clazz, msg, null);
    }

    public static void warning(String msg, Throwable cause) {
        warning(null, msg, cause);
    }

    public static void warning(Class<?> clazz, String msg, Throwable cause) {
        if (logLevel.ordinal() <= Level.WARNING.ordinal()) {
            systemPrintln(clazz, "WARNING", msg, cause);
        }
    }

    public static void info(String msg) {
        info(msg, null);
    }

    public static void info(Class<?> clazz, String msg) {
        info(clazz, msg, null);
    }

    public static void info(String msg, Throwable cause) {
        info(null, msg, cause);
    }

    public static void info(Class<?> clazz, String msg, Throwable cause) {
        if (logLevel.ordinal() <= Level.INFO.ordinal()) {
            systemPrintln(clazz, "INFO", msg, cause);
        }
    }

    public static void debug(String msg) {
        debug(msg, null);
    }

    public static void debug(Class<?> clazz, String msg) {
        debug(clazz, msg, null);
    }

    public static void debug(String msg, Throwable cause) {
        debug(null, msg, cause);
    }

    public static void debug(Class<?> clazz, String msg, Throwable cause) {
        if (logLevel != Level.DEBUG) return;
        systemPrintln(clazz, "DEBUG", msg, cause);
    }

    private static void systemPrintln(Class<?> clazz, String Level, String msg, Throwable cause) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String printMsg = format.format(new Date());
        printMsg += " " + Level;
        if (clazz != null) {
            printMsg += " [" + clazz.getName() + "]";
        }
        printMsg += (msg == null ? "" : " " + msg);
        if (cause != null) {
            printMsg += " " + cause.getMessage();
        }

        System.out.println(printMsg);
    }

    public enum Level {
        DEBUG,
        INFO,
        WARNING,
        ERROR
    }

    public static Level getLogLevel(String str) {
        if (str.toUpperCase().equals("DEBUG")) {
            return Level.DEBUG;
        } else if (str.toUpperCase().equals("INFO")) {
            return Level.INFO;
        } else if (str.toUpperCase().equals("WARNING")) {
            return Level.WARNING;
        } else if (str.toUpperCase().equals("ERROR")) {
            return Level.ERROR;
        }

        return Level.DEBUG;
    }

}
