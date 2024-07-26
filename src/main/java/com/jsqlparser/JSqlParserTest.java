package com.jsqlparser;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import org.junit.Test;

public class JSqlParserTest {

    /**
     * 可以使用jsqlparser实现一个sql解析计算引擎
     */
    @Test
    public void test() throws Exception {
        String sql = "select a, substr(b, 1, 10) dt, substr(nvl(b, '2024'), 1, 10) dt2, c c2, 1 d, '1' e from table";
        Statement statement = CCJSqlParserUtil.parse(sql);
        System.out.println(statement);
        sql = "select a, substr(b, 1, 10) dt, c as c2, 1 d, '1' e";
        statement = CCJSqlParserUtil.parse(sql);
        System.out.println(statement);
        sql = "a, substr(b, 1, 10) dt, c ";
        Expression expression = CCJSqlParserUtil.parseExpression(sql);
        System.out.println(expression);
        sql = "substr(b, 1, 10) dt";
        expression = CCJSqlParserUtil.parseExpression(sql);
        System.out.println(expression);
        sql = "c as c2";
        expression = CCJSqlParserUtil.parseExpression(sql);
        System.out.println(expression);
    }
}
