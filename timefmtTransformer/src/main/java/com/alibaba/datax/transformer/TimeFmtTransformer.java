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
        String defaultValue = null;//传时间戳

        try {
            if (paras.length != 3 && paras.length != 2) {
                throw new RuntimeException("time_fmt paras must be 2 or 3");
            }

            columnIndex = (Integer) paras[0];
            type = Integer.valueOf((String) paras[1]);
            if(paras.length == 3){
                defaultValue = (String) paras[2];
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
                    record.setColumn(columnIndex, new DateColumn(Long.parseLong(time)));
                }
            } else if (type == 2) {
                if(column.getType() == Column.Type.DATE){
                    Date date = column.asDate();
                    if(date == null && StringUtils.isNotBlank(defaultValue)){
                        date = new Date(Long.parseLong(defaultValue));
                    }
                    if(date != null){
                        record.setColumn(columnIndex, new LongColumn(date.getTime()));
                    }
                }
            }

        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(),e);
        }
        return record;
    }

}
