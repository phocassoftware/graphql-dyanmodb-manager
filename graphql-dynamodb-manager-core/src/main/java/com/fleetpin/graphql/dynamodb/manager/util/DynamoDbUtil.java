package com.fleetpin.graphql.dynamodb.manager.util;

import com.fleetpin.graphql.dynamodb.manager.table.Table;
import com.fleetpin.graphql.dynamodb.manager.table.TableName;

public final class DynamoDbUtil {
    public static String table(Class<? extends Table> type) {
        Class<?> tmp = type;
        TableName name = null;
        while(name == null && tmp != null) {
            name = tmp.getDeclaredAnnotation(TableName.class);
            tmp = tmp.getSuperclass();
        }
        if(name == null) {
            return type.getSimpleName().toLowerCase() + "s";
        } else {
            return name.value();
        }
    }
}