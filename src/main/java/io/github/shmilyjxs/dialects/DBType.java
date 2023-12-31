package io.github.shmilyjxs.dialects;

import java.util.Arrays;

public enum DBType {
    ORACLE("Oracle", new OracleDialect()),
    MYSQL("MySQL", new MySQLDialect()),
    POSTGRESQL("PostgreSQL", new PostgreSQLDialect());

    private final String productName;
    private final IDialect dialect;

    DBType(String productName, IDialect dialect) {
        this.productName = productName;
        this.dialect = dialect;
    }

    public static DBType fromProductName(String productName) {
        return Arrays.stream(values()).filter(e -> e.getProductName().equalsIgnoreCase(productName)).findAny().orElse(null);
    }

    public String getProductName() {
        return productName;
    }

    public IDialect getDialect() {
        return dialect;
    }
}