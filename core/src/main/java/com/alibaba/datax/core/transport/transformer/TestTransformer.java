package com.alibaba.datax.core.transport.transformer;

import static com.alibaba.datax.core.transport.transformer.GroovyTransformerStaticUtil.*;
import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.transformer.Transformer;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

/**
 * no comments.
 *
 * @author XuDaojie
 * @since 2021-08-16
 */
public class TestTransformer extends Transformer {


    @Override
    public Record evaluate(Record record, Object... paras) {
        Column column0 = record.getColumn(0);
        Column column1 = record.getColumn(1);
        Column column2 = record.getColumn(2);
        Record rcd = new DefaultRecord();
        String str1 = column1.asString();
        String str2 = column2.asString();
        rcd.addColumn(column0);
        rcd.addColumn(new StringColumn(str1 + str2));

        BigDecimal num1 = column0.asBigDecimal();
        BigDecimal num2 = column2.asBigDecimal();
        rcd.addColumn(new DoubleColumn(num1.add(num2).multiply(num1)));

        //获取列对象，0表示第一列
        Column column = record.getColumn(0);
        //转成String
        String strValue = column.asString();
        //转成Date
        Date dateValue = column.asDate();
        Date dateValue1 = column.asDate("yyyy-MM-dd HH:mm:ss");
        //转成数字
        Long longValue = column.asLong();
        BigDecimal bigDecimalValue = column.asBigDecimal();
        BigInteger integerValue = column.asBigInteger();
        Double doubleValue = column.asDouble();
        //转成boolean
        Boolean booleanValue = column.asBoolean();
        //转成Bytes
        byte[] bytesValue = column.asBytes();
        //处理列的值：其他类型的列（DoubleColumn、DateColumn、LongColumn、BoolColumn、BytesColumn）
        Column targetColumn = new StringColumn(strValue + "这里自定义对strValue操作生成新的值");
        //将转换处理后的值设回
        record.setColumn(0, targetColumn);
        return record;
    }

}
