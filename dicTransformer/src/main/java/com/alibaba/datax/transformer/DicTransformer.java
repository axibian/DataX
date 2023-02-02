package com.alibaba.datax.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.transport.transformer.TransformerErrorCode;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by Liu Kun on 2019/3/14.
 */
public class DicTransformer extends ComplexTransformer{

    public DicTransformer(){
        super.setTransformerName("dic_fmt");
    }

    public Record evaluate(Record record, Map<String, Object> tContext, Object... paras) {
        int columnIndex;
        String typeCode;//bsc数据字典typeCode

        try {
            if (paras.length != 2) {
                throw new RuntimeException("dic_fmt paras must be 2");
            }

            columnIndex = (Integer) paras[0];
            typeCode = (String) paras[1];

        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }

        if(tContext != null){
            Column column = record.getColumn(columnIndex);
            try {
                String oriValue = column.asString();
                if(StringUtils.isNotBlank(oriValue)){
                    JSONObject obj = (JSONObject) tContext.get(typeCode);
                    if(obj != null){
                        String newValue = obj.getString(oriValue);
                        if(StringUtils.isNotBlank(newValue)){
                            record.setColumn(columnIndex, new StringColumn(newValue));
                        }
                    }
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(),e);
            }
        }

        return record;
    }

}
