package Irisa.Enssat.Rennes1;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by letrungdung on 26/02/2018.
 */
public class TestSQL {
    static String HOME = System.getenv().get("HOME");
    static String FILEUSER = HOME + "/username.txt";
    static String username = com.sparkexample.TestPostgreSQLDatabase.readpass(FILEUSER);

    static String FILENAME = HOME + "/password.txt";
    static String password = com.sparkexample.TestPostgreSQLDatabase.readpass(FILENAME);
    static String query4 = IRES.TPCHQuery.readSQL(HOME+"/SQL/tpch_query4");
    public static void main(String arg[]) throws JSQLParserException {
        test();
    }
    public static void test() throws JSQLParserException {
        String SQL = "SELECT * FROM lineitem, orders, customer WHERE l_orderkey = o_orderkey AND c_orderkey = l_orderkey AND c_orderkey = o_orderkey";
        //SQL = query4;
        String sql = "SELECT * FROM MY_TABLE1, MY_TABLE2, (SELECT * FROM MY_TABLE3) LEFT OUTER JOIN MY_TABLE4 "+
                " WHERE ID = (SELECT MAX(ID) FROM MY_TABLE5) AND ID2 IN (SELECT * FROM MY_TABLE6)" ;
        sql = SQL;
        System.out.println(sql);
/*
        CCJSqlParserManager pm = new CCJSqlParserManager();
        Statement statement = pm.parse(new StringReader(sql));
        if (statement instanceof Select) {
            Select selectStatement = (Select) statement;
            TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
            List tableList = tablesNamesFinder.getTableList(selectStatement);
            for (Iterator iter = tableList.iterator(); iter.hasNext();) {
                System.out.println(tableList);
            }
        }
*/
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
        List<String> tableList = tablesNamesFinder.getTableList(selectStatement);
        System.out.println(tableList);

        PlainSelect ps = (PlainSelect) selectStatement.getSelectBody();
        AndExpression e = (AndExpression) ps.getWhere();

        ps.getJoins();

        //System.out.println(ps.getJoins());
        //System.out.println(ps.getSelectItems());
        //System.out.println(ps.getWhere().toString());
        //System.out.println(e.getLeftExpression());
        //System.out.println(e.getRightExpression());

        //List<AndExpression> andExpressionsList = RightExpression(e);
        //Expression exc = e.getLeftExpression();
        //e.getStringExpression();
        System.out.println(AndExpression(e));
        //selectStatement.getSelectBody();
        //ExcludeConstraint excludeConstraint = new ExcludeConstraint();
        //System.out.println("");
    }
    public static List<String> AndExpression(AndExpression andExpression){
        String[] arrayAndExpression = andExpression.toString().split("AND");
        List<String> listAndExpression = new ArrayList<>();
        for (int i = 0; i< arrayAndExpression.length; i++){
            listAndExpression.add(arrayAndExpression[i]);
        }
        return listAndExpression;
    }
}
