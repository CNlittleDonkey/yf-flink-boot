package com.yf.until;

import org.apache.flink.table.functions.ScalarFunction;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

/**
 * @ClassName ContainFun
 * @Description TODO
 * @Author xuhaoYF501492
 * @Date 2024/6/21 15:37
 * @Version 1.0
 */
public class ContainFun extends ScalarFunction {

    public static int calculateQualityCode(BigDecimal paramValue, String invalidValue, BigDecimal rangeUpper, BigDecimal rangeLower) {
        if (valFun(paramValue.toString(), invalidValue)) {
            return 2;
        } else if (rangeUpper.compareTo(paramValue) < 0 || rangeLower.compareTo(paramValue) > 0) {
            return 1;
        } else if (!valFun(paramValue.toString(), invalidValue) && rangeUpper.compareTo(paramValue) >= 0 && rangeLower.compareTo(paramValue) <= 0) {
            return 0;
        } else {
            return 3;
        }
    }

    public static Boolean valFun(String source, String target) {
        if (target != null && !target.trim().isEmpty()) {
            Optional<Float> any = Arrays.stream(target.split(","))
                    .map(Float::valueOf)
                    .filter(s -> s.equals(Float.valueOf(source)))
                    .findAny();
            return any.isPresent();
        }
        return Boolean.FALSE;

    }

    public static long parseLong(String value) {
        return value == null ? 0L : Long.parseLong(value);
    }

    public static BigDecimal parseBigDecimal(String value) {
        if (value == null || value.isEmpty()) {
            return BigDecimal.ZERO;
        }
        try {
            return new BigDecimal(value);
        } catch (NumberFormatException e) {
            System.out.println("-----------" + value);
            e.printStackTrace(); // 打印堆栈信息以便调试
            return BigDecimal.ZERO; // 或者根据你的业务逻辑选择其他默认值
        }
    }

    public static boolean parseBoolean(String value) {
        return value != null && Boolean.parseBoolean(value);
    }

    public static  long calculateMeasNo(long time) {
        return Timestamp.valueOf("1970-01-01 00:00:00").toLocalDateTime()
                .until(Instant.ofEpochMilli(time).atZone(ZoneId.systemDefault()).toLocalDateTime(), ChronoUnit.HOURS);
    }


}