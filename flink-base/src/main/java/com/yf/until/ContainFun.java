package com.yf.until;

import org.apache.flink.table.functions.ScalarFunction;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Optional;

/**
 * @ClassName ContainFun
 * @Description TODO
 * @Author xuhaoYF501492
 * @Date 2024/6/21 15:37
 * @Version 1.0
 */
public  class ContainFun extends ScalarFunction  {

    public static int calculateQualityCode(BigDecimal paramValue, String invalidValue,BigDecimal rangeUpper, BigDecimal rangeLower) {
        if (valFun(paramValue.toString(), invalidValue)) {
              return 2;
        } else if (rangeUpper.compareTo(paramValue) < 0 || rangeLower.compareTo(paramValue) > 0) {
              return 1;
        } else if (!valFun(paramValue.toString(), invalidValue) && rangeUpper.compareTo(paramValue) >= 0 && rangeLower.compareTo(paramValue) <= 0) {
           return  0;
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
}