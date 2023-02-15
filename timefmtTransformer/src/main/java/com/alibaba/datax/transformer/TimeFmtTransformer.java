package com.alibaba.datax.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.transport.transformer.TransformerErrorCode;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Date;

/**
 * Created by Liu Kun on 2019/3/14.
 */
public class TimeFmtTransformer extends Transformer{

    public TimeFmtTransformer(){
        super.setTransformerName("time_fmt");
    }


    public Record evaluate(Record record, Object... paras){
        int columnIndex;
        int type;//1-转成Date；2-转成时间戳
        int timestampUnit;//时间戳单位：1-秒；2-毫秒
        String defaultValue = null;//传时间戳

        try {
            if (paras.length != 3 && paras.length != 4) {
                throw new RuntimeException("time_fmt paras must be 3 or 4");
            }

            columnIndex = (Integer) paras[0];
            type = Integer.valueOf((String) paras[1]);
            timestampUnit = Integer.valueOf((String) paras[2]);
            if(paras.length == 4){
                defaultValue = (String) paras[3];
            }

        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }

        Column column = record.getColumn(columnIndex);

        try {
            if (type == 1) {
                String time = column.asString();
                if (time == null && StringUtils.isNotBlank(defaultValue)) {
                    time = defaultValue;
                }
                if(time != null){
                    Long timel = Long.parseLong(time);
                    if((timel < 10000000000l && timel>0) || timel == -28800){//秒的转成毫秒
                        timel *= 1000;
                    }
                    record.setColumn(columnIndex, new DateColumn(timel));
                }
            } else if (type == 2) {
                if(column.getType() == Column.Type.DATE){
                    Date date = column.asDate();
                    if(date != null){
                        long timel = date.getTime();
                        if(timestampUnit == 1){
                            timel /= 1000;
                        }
                        record.setColumn(columnIndex, new LongColumn(timel));
                    }else{
                        if(StringUtils.isNotBlank(defaultValue)){
                            record.setColumn(columnIndex, new LongColumn(Long.parseLong(defaultValue)));
                        }
                    }
                }
            }

        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(),e);
        }
        return record;
    }

}
