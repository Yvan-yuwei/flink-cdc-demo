package com.lepin.enums;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public enum TypesToSchemaEnums {

    LONG("Long", Types.LongType.get()),
    STRING("String", Types.StringType.get()),
    BYTE("Byte", Types.IntegerType.get()),
    INTEGER("Integer", Types.IntegerType.get()),
    LOCAL_DATE_TIME("LocalDateTime", Types.TimestampType.withZone()),
    FLOAT("Float", Types.FloatType.get()),
    DECIMAL("Decimal", Types.DecimalType.of(38,20)),
    DOUBLE("Double", Types.DoubleType.get()),
    BOOLEAN("Boolean", Types.BooleanType.get()),
    DATE("Date", Types.DateType.get()),
    TIME("Time", Types.TimeType.get()),


    ;

    private final String code;
    private final Type nestedField;

    TypesToSchemaEnums(String code, Type nestedField) {
        this.code = code;
        this.nestedField = nestedField;
    }

    public String getCode() {
        return code;
    }

    public static Type of(String code) {
        for (TypesToSchemaEnums value : TypesToSchemaEnums.values()) {
            if (value.getCode().equals(code)) {
                return value.nestedField;
            }
        }
        return Types.StringType.get();
    }

}
