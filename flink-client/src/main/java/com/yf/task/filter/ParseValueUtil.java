package com.yf.task.filter;

import com.yf.task.pojo.EnergyStorageDimension;
import com.yf.task.pojo.EnrichedStatMutation;
import com.yf.task.pojo.StatMutation;

import static com.yf.until.ContainFun.calculateMeasNo;

/**
 * @ClassName ParseValueUtil
 * @Description TODO
 * @Author xuhaoYF501492
 * @Date 2024/6/28 14:42
 * @Version 1.0
 */
public class ParseValueUtil {

    public static EnrichedStatMutation createEnrichedStatMutation(StatMutation input, EnergyStorageDimension dim) {
        return new EnrichedStatMutation(
                dim.getMeasuringId(),
                dim.getAggrStationId(),
                dim.getAggrStationCode(),
                dim.getAggrStationName(),
                dim.getStationId(),
                dim.getStationCode(),
                dim.getStationName(),
                dim.getStationAbbr(),
                dim.getStationTypeId(),
                dim.getStationTypeCode(),
                dim.getInterStation(),
                dim.getCabinetNo(),
                dim.getStaCapacity(),
                dim.getTypeId(),
                dim.getTypeCode(),
                dim.getTypeName(),
                dim.getLogicEquId(),
                dim.getLogicEquCode(),
                dim.getLogicEquName(),
                dim.getEmuSn(),
                dim.getIndicatorTempId(),
                dim.getModel(),
                dim.getNoAlm(),
                dim.getMainAdvise(),
                dim.getScript(),
                dim.getRelateParamCode(),
                dim.getInterEqu(),
                dim.getCustView(),
                dim.getCustAlmName(),
                dim.getInvalidValue(),
                dim.getRangeUpper(),
                dim.getRangeLower(),
                dim.getMsgRuleId(),
                dim.getAlmClaz(),
                dim.getAlmLevel(),
                dim.getFaultMonitor(),
                calculateMeasNo(input.getTime()),
                dim.getParamSn(),
                dim.getParamId(),
                dim.getParamCode(),
                dim.getParamType(),
                dim.getParamName(),
                dim.getParamClaz(),
                input.getRaw(),
                dim.getCoef(),
                input.getTime(),
                dim.getRecovery(),
                dim.getStatus(),
                dim.getTenantId()
        );
    }
}
