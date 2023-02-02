package com.alibaba.datax.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.transport.transformer.TransformerErrorCode;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;

/**
 * Created by Liu Kun on 2019/3/14.
 */
public class DecimalFmtTransformer extends Transformer{

    public DecimalFmtTransformer(){
        super.setTransformerName("decimal_fmt");
    }


    public Record evaluate(Record record, Object... paras){
        int columnIndex;
        int precision;//精度
        int scaleType;//1-四舍五入；2-截断；3-进一法
        String defaultValue = null;

        try {
            if (paras.length != 4 && paras.length != 3) {
                throw new RuntimeException("decimal_fmt paras must be 3 or 4");
            }

            columnIndex = (Integer) paras[0];
            precision = Integer.valueOf((String) paras[1]);
            scaleType = Integer.valueOf((String) paras[2]);
            if (paras.length == 4){
                defaultValue = (String) paras[3];
            }

        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }

        Column column = record.getColumn(columnIndex);

        try {
            BigDecimal num = column.asBigDecimal();
            if (num == null && StringUtils.isNotBlank(defaultValue)) {
                num = new BigDecimal(defaultValue);
            }
            if(num != null){
                if(scaleType == 1){
                    num = num.setScale(precision, RoundingMode.HALF_UP);
                }else if(scaleType == 2){
                    num = num.setScale(precision, RoundingMode.DOWN);
                }else if(scaleType == 3){
                    num = num.setScale(precision, RoundingMode.UP);
                }
                record.setColumn(columnIndex, new DoubleColumn(num));
            }

        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(),e);
        }
        return record;
    }

}
