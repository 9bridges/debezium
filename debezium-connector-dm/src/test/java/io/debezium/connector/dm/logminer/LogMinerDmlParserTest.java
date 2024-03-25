/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dm.logminer;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.dm.logminer.parser.LogMinerDmlParser;
import io.debezium.connector.dm.logminer.valueholder.LogMinerDmlEntry;
import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * @author Chris Cranford
 */
public class LogMinerDmlParserTest {

    private LogMinerDmlParser fastDmlParser;

    @Before
    public void beforeEach() throws Exception {
        // Create LogMinerDmlParser
        fastDmlParser = new LogMinerDmlParser();
    }

    // Oracle's generated SQL avoids common spacing patterns such as spaces between column values or values
    // in an insert statement and is explicit about spacing and commas with SET and WHERE clauses. As of
    // now the parser expects this explicit spacing usage.

    @Test
    @FixFor("DBZ-3078")
    public void testParsingInsert() throws Exception {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.TEST"))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("NAME").create())
                .addColumn(Column.editor().name("TS").create())
                .addColumn(Column.editor().name("UT").create())
                .addColumn(Column.editor().name("DATE").create())
                .addColumn(Column.editor().name("UT2").create())
                .addColumn(Column.editor().name("C1").create())
                .addColumn(Column.editor().name("C2").create())
                .addColumn(Column.editor().name("UNUSED").create())
                .create();
        // DELETE FROM "TEST"."T1" WHERE "TINT" = 142672 AND "TFLOAT" = 472483.000000 AND "TDOUBLE" = 851879.000000 AND "TTIMESTAMP" = TIMESTAMP'1999-08-31 12:05:08.974' AND "TDATE" = DATE'2007-06-13';
        String sql = "INSERT INTO \"TEST\".\"oracle_T1\"(\"TCHAR\", \"TINT\", \"TVARCHAR\", \"TSMALLINT\", \"TBIGINT\", \"TREAL\", \"TFLOAT\", \"TDOUBLE\", \"TDECIMAL\", \"TCLOB\", \"TBLOB\", \"TDATE\", \"TTIMESTAMP\", \"TNUMBER\", \"TINTEGER\") VALUES('t', 214194, 'over the lazy dogThe quick brown fox ju', 22043, 968311, 153997.000000, 602081.000000, 27563.000000, 367870, NULL, NULL, DATE'2015-10-11', TIMESTAMP'1989-11-02 00:29:22.397', 1, 423009);";

        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table, null);
        assertThat(entry.getOperation()).isEqualTo(RowMapper.INSERT);
        assertThat(entry.getOldValues()).isEmpty();
        assertThat(entry.getNewValues()).hasSize(9);
        assertThat(entry.getNewValues()[0]).isEqualTo("1");
        assertThat(entry.getNewValues()[1]).isEqualTo("Acme");
        assertThat(entry.getNewValues()[2]).isEqualTo("1999-08-31 12:05:08");
        assertThat(entry.getNewValues()[3]).isNull();
        assertThat(entry.getNewValues()[4]).isEqualTo("2020-02-01 00:00:00");
        assertThat(entry.getNewValues()[5]).isNull();
        assertThat(entry.getNewValues()[6]).isNull();
        assertThat(entry.getNewValues()[7]).isNull();
        assertThat(entry.getNewValues()[8]).isNull();
    }

    @Test
    @FixFor("DBZ-3078")
    public void testParsingUpdate() throws Exception {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.TEST"))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("NAME").create())
                .addColumn(Column.editor().name("TS").create())
                .addColumn(Column.editor().name("UT").create())
                .addColumn(Column.editor().name("DATE").create())
                .addColumn(Column.editor().name("UT2").create())
                .addColumn(Column.editor().name("C1").create())
                .addColumn(Column.editor().name("IS").create())
                .addColumn(Column.editor().name("IS2").create())
                .addColumn(Column.editor().name("UNUSED").create())
                .create();

        String sql = "UPDATE \"DEBEZIUM\".\"TEST\" " +
                "SET \"NAME\" = 'BOB', \"TS\" = TIMESTAMP'2020-02-02 00:00:00', \"UT\" = Unsupported Type, " +
                "\"DATE\" = DATE'2020-02-02 00:00:00', \"UT2\" = Unsupported Type, " +
                "\"C1\" = NULL WHERE \"ID\" = '1' AND \"NAME\" = 'ACME' AND \"TS\" = TIMESTAMP'2020-02-01 00:00:00' AND " +
                "\"UT\" = Unsupported Type AND \"DATE\" = DATE'2020-02-01 00:00:00' AND " +
                "\"UT2\" = Unsupported Type AND \"C1\" = NULL AND \"IS\" IS NULL AND \"IS2\" IS NULL;";

        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table, null);
        assertThat(entry.getOperation()).isEqualTo(RowMapper.UPDATE);
        assertThat(entry.getOldValues()).hasSize(10);
        assertThat(entry.getOldValues()[0]).isEqualTo("1");
        assertThat(entry.getOldValues()[1]).isEqualTo("ACME");
        assertThat(entry.getOldValues()[2]).isEqualTo("2020-02-01 00:00:00");
        assertThat(entry.getOldValues()[3]).isNull();
        assertThat(entry.getOldValues()[4]).isEqualTo("2020-02-01 00:00:00");
        assertThat(entry.getOldValues()[5]).isNull();
        assertThat(entry.getOldValues()[6]).isNull();
        assertThat(entry.getOldValues()[7]).isNull();
        assertThat(entry.getOldValues()[8]).isNull();
        assertThat(entry.getOldValues()[9]).isNull();
        assertThat(entry.getNewValues()).hasSize(10);
        assertThat(entry.getNewValues()[0]).isEqualTo("1");
        assertThat(entry.getNewValues()[1]).isEqualTo("BOB");
        assertThat(entry.getNewValues()[2]).isEqualTo("2020-02-02 00:00:00");
        assertThat(entry.getNewValues()[3]).isNull();
        assertThat(entry.getNewValues()[4]).isEqualTo("2020-02-02 00:00:00");
        assertThat(entry.getNewValues()[5]).isNull();
        assertThat(entry.getNewValues()[6]).isNull();
        assertThat(entry.getNewValues()[7]).isNull();
        assertThat(entry.getNewValues()[8]).isNull();
        assertThat(entry.getNewValues()[9]).isNull();
    }

    @Test
    @FixFor("DBZ-3078")
    public void testParsingDelete() throws Exception {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.TEST"))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("NAME").create())
                .addColumn(Column.editor().name("TS").create())
                .addColumn(Column.editor().name("UT").create())
                .addColumn(Column.editor().name("DATE").create())
                .addColumn(Column.editor().name("IS").create())
                .addColumn(Column.editor().name("IS2").create())
                .addColumn(Column.editor().name("UNUSED").create())
                .create();

        String sql = "DELETE FROM \"DEBEZIUM\".\"TEST\" " +
                "WHERE \"ID\" = '1' AND \"NAME\" = 'Acme' AND \"TS\" = TIMESTAMP'2021-02-01 00:00:00' AND " +
                "\"UT\" = Unsupported Type AND \"DATE\" = DATE'2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS' AND " +
                "\"IS\" IS NULL AND \"IS2\" IS NULL;";

        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table, null);
        assertThat(entry.getOperation()).isEqualTo(RowMapper.DELETE);
        assertThat(entry.getOldValues()).hasSize(8);
        assertThat(entry.getOldValues()[0]).isEqualTo("1");
        assertThat(entry.getOldValues()[1]).isEqualTo("Acme");
        assertThat(entry.getOldValues()[2]).isEqualTo("2021-02-01 00:00:00");
        assertThat(entry.getOldValues()[3]).isNull();
        assertThat(entry.getOldValues()[4]).isEqualTo("2020-02-01 00:00:00");
        assertThat(entry.getOldValues()[5]).isNull();
        assertThat(entry.getOldValues()[6]).isNull();
        assertThat(entry.getOldValues()[7]).isNull();
        assertThat(entry.getNewValues()).isEmpty();
    }

    @Test
    @FixFor({ "DBZ-3235", "DBZ-4194" })
    public void testParsingUpdateWithNoWhereClauseIsAcceptable() throws Exception {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.TEST"))
                .addColumn(Column.editor().name("COL1").create())
                .addColumn(Column.editor().name("COL2").create())
                .addColumn(Column.editor().name("COL3").create())
                .addColumn(Column.editor().name("UNUSED").create())
                .create();

        String sql = "update \"DEBEZIUM\".\"TEST\" set \"COL1\" = '1', \"COL2\" = NULL, \"COL3\" = 'Hello';";

        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table, null);
        assertThat(entry.getOperation()).isEqualTo(RowMapper.UPDATE);
        assertThat(entry.getOldValues()).hasSize(4);
        assertThat(entry.getOldValues()[0]).isNull();
        assertThat(entry.getOldValues()[1]).isNull();
        assertThat(entry.getOldValues()[2]).isNull();
        assertThat(entry.getOldValues()[3]).isNull();
        assertThat(entry.getNewValues()).hasSize(4);
        assertThat(entry.getNewValues()[0]).isEqualTo("1");
        assertThat(entry.getNewValues()[1]).isNull();
        assertThat(entry.getNewValues()[2]).isEqualTo("Hello");
        assertThat(entry.getNewValues()[3]).isNull();
    }

    @Test
    @FixFor("DBZ-3235")
    public void testParsingDeleteWithNoWhereClauseIsAcceptable() throws Exception {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.TEST"))
                .addColumn(Column.editor().name("COL1").create())
                .addColumn(Column.editor().name("COL2").create())
                .addColumn(Column.editor().name("COL3").create())
                .addColumn(Column.editor().name("UNUSED").create())
                .create();

        String sql = "delete from \"DEBEZIUM\".\"TEST\";";

        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table, null);
        assertThat(entry.getOperation()).isEqualTo(RowMapper.DELETE);
        assertThat(entry.getOldValues()).hasSize(4);
        assertThat(entry.getOldValues()[0]).isNull();
        assertThat(entry.getOldValues()[1]).isNull();
        assertThat(entry.getOldValues()[2]).isNull();
        assertThat(entry.getOldValues()[3]).isNull();
        assertThat(entry.getNewValues()).isEmpty();
    }

    @Test
    @FixFor("DBZ-4194")
    public void testParsingWithTableAliases() throws Exception {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.TEST"))
                .addColumn(Column.editor().name("COL1").create())
                .addColumn(Column.editor().name("COL2").create())
                .addColumn(Column.editor().name("COL3").create())
                .addColumn(Column.editor().name("UNUSED").create())
                .create();

        String sql = "update \"DEBEZIUM\".\"TEST\" a set a.\"COL1\" = '1', a.\"COL2\" = NULL, a.\"COL3\" = 'Hello2';";

        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table, null);
        assertThat(entry.getOperation()).isEqualTo(RowMapper.UPDATE);
        assertThat(entry.getNewValues()[0]).isEqualTo("1");
        assertThat(entry.getNewValues()[1]).isNull();
        assertThat(entry.getNewValues()[2]).isEqualTo("Hello2");

        sql = "delete from \"DEBEZIUM\".\"TEST\" a where a.\"COL1\" = '1' and a.\"COL2\" = '2' and a.\"COL3\" = Unsupported Type;";

        entry = fastDmlParser.parse(sql, table, null);
        assertThat(entry.getOperation()).isEqualTo(RowMapper.DELETE);
        assertThat(entry.getOldValues()).hasSize(4);
        assertThat(entry.getOldValues()[0]).isEqualTo("1");
        assertThat(entry.getOldValues()[1]).isEqualTo("2");
        assertThat(entry.getOldValues()[2]).isNull();
        assertThat(entry.getOldValues()[3]).isNull();
        assertThat(entry.getNewValues()).isEmpty();
    }

    @Test
    @FixFor("DBZ-3258")
    public void testNameWithWhitespaces() throws Exception {
        final Table table = Table.editor()
                .tableId(new TableId(null, "UNKNOWN", "OBJ# 74858"))
                .addColumn(Column.editor().name("COL 1").create())
                .create();

        String sql = "INSERT INTO \"UNKNOWN\".\"OBJ# 74858\"(\"COL 1\") VALUES (1)";

        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table, null);
        assertThat(entry.getOperation()).isEqualTo(RowMapper.INSERT);
        assertThat(entry.getOldValues()).isEmpty();
        assertThat(entry.getNewValues()).hasSize(1);
        assertThat(entry.getNewValues()[0]).isEqualTo("1");
    }

    @Test
    @FixFor("DBZ-3305")
    public void testParsingUpdateWithNoWhereClauseFunctionAsLastColumn() throws Exception {
        final Table table = Table.editor()
                .tableId(new TableId(null, "TICKETUSER", "CRS_ORDER"))
                .addColumn(Column.editor().name("AMOUNT_PAID").create())
                .addColumn(Column.editor().name("AMOUNT_UNPAID").create())
                .addColumn(Column.editor().name("PAY_STATUS").create())
                .addColumn(Column.editor().name("IS_DEL").create())
                .addColumn(Column.editor().name("TM_UPDATE").create())
                .create();
        String sql = "UPDATE \"TICKETUSER\".\"CRS_ORDER\" SET \"AMOUNT_PAID\" = '0', \"AMOUNT_UNPAID\" = '540', " +
                "\"PAY_STATUS\" = '10111015', \"IS_DEL\" = '0', \"TM_UPDATE\" = DATE'2021-03-17 10:18:55';";

        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table, null);
        assertThat(entry.getOperation()).isEqualTo(RowMapper.UPDATE);
        assertThat(entry.getOldValues()).hasSize(5);
        assertThat(entry.getOldValues()[0]).isNull();
        assertThat(entry.getOldValues()[1]).isNull();
        assertThat(entry.getOldValues()[2]).isNull();
        assertThat(entry.getOldValues()[3]).isNull();
        assertThat(entry.getOldValues()[4]).isNull();
        assertThat(entry.getNewValues()).hasSize(5);
        assertThat(entry.getNewValues()[0]).isEqualTo("0");
        assertThat(entry.getNewValues()[1]).isEqualTo("540");
        assertThat(entry.getNewValues()[2]).isEqualTo("10111015");
        assertThat(entry.getNewValues()[3]).isEqualTo("0");
        assertThat(entry.getNewValues()[4]).isEqualTo("2021-03-17 10:18:55");
    }

    @Test
    @FixFor("DBZ-3367")
    public void shouldParsingRedoSqlWithParenthesisInFunctionArgumentStrings() throws Exception {
        final Table table = Table.editor()
                .tableId(new TableId(null, "DEBEZIUM", "TEST"))
                .addColumn(Column.editor().name("C1").create())
                .addColumn(Column.editor().name("C2").create())
                .create();

        String sql = "INSERT INTO \"DEBEZIUM\".\"TEST\" (\"C1\", \"C2\") VALUES(UNISTR('\\963F\\72F8\\5C0F\\706B\\8F66\\5BB6\\5EAD\\7968(\\60CA\\559C\\FF09\\FF082161\\FF09'), NULL);";

        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table, null);
        assertThat(entry.getOperation()).isEqualTo(RowMapper.INSERT);
        assertThat(entry.getOldValues()).isEmpty();
        assertThat(entry.getNewValues()).hasSize(2);
        assertThat(entry.getNewValues()[0])
                .isEqualTo("UNISTR('\\963F\\72F8\\5C0F\\706B\\8F66\\5BB6\\5EAD\\7968(\\60CA\\559C\\FF09\\FF082161\\FF09')");
        assertThat(entry.getNewValues()[1]).isNull();

        sql = "UPDATE \"DEBEZIUM\".\"TEST\" SET " +
                "\"C2\" = UNISTR('\\963F\\72F8\\5C0F\\706B\\8F66\\5BB6\\5EAD\\7968(\\60CA\\559C\\FF09\\FF082161\\FF09') " +
                "WHERE \"C1\" = UNISTR('\\963F\\72F8\\5C0F\\706B\\8F66\\5BB6\\5EAD\\7968(\\60CA\\559C\\FF09\\FF082161\\FF09');";
        entry = fastDmlParser.parse(sql, table, null);
        assertThat(entry.getOperation()).isEqualTo(RowMapper.UPDATE);
        assertThat(entry.getOldValues()).hasSize(2);
        assertThat(entry.getOldValues()[0])
                .isEqualTo("UNISTR('\\963F\\72F8\\5C0F\\706B\\8F66\\5BB6\\5EAD\\7968(\\60CA\\559C\\FF09\\FF082161\\FF09')");
        assertThat(entry.getOldValues()[1]).isNull();
        assertThat(entry.getNewValues()).hasSize(2);
        assertThat(entry.getNewValues()[0])
                .isEqualTo("UNISTR('\\963F\\72F8\\5C0F\\706B\\8F66\\5BB6\\5EAD\\7968(\\60CA\\559C\\FF09\\FF082161\\FF09')");
        assertThat(entry.getNewValues()[1])
                .isEqualTo("UNISTR('\\963F\\72F8\\5C0F\\706B\\8F66\\5BB6\\5EAD\\7968(\\60CA\\559C\\FF09\\FF082161\\FF09')");

        sql = "DELETE FROM \"DEBEZIUM\".\"TEST\" WHERE \"C1\" = UNISTR('\\963F\\72F8\\5C0F\\706B\\8F66\\5BB6\\5EAD\\7968(\\60CA\\559C\\FF09\\FF082161\\FF09');";
        entry = fastDmlParser.parse(sql, table, null);
        assertThat(entry.getOperation()).isEqualTo(RowMapper.DELETE);
        assertThat(entry.getOldValues()).hasSize(2);
        assertThat(entry.getOldValues()[0])
                .isEqualTo("UNISTR('\\963F\\72F8\\5C0F\\706B\\8F66\\5BB6\\5EAD\\7968(\\60CA\\559C\\FF09\\FF082161\\FF09')");
        assertThat(entry.getOldValues()[1]).isNull();
        assertThat(entry.getNewValues()).isEmpty();
    }

    @Test
    @FixFor("DBZ-3413")
    public void testParsingDoubleSingleQuoteInWhereClause() throws Exception {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.TEST"))
                .addColumn(Column.editor().name("COL1").create())
                .addColumn(Column.editor().name("COL2").create())
                .create();

        String sql = "INSERT INTO \"DEBEZIUM\".\"TEST\" (\"COL1\", \"COL2\") VALUES('Bob''s dog', '0');";
        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table, null);
        assertThat(entry.getOperation()).isEqualTo(RowMapper.INSERT);
        assertThat(entry.getOldValues()).isEmpty();
        assertThat(entry.getNewValues()).hasSize(2);
        assertThat(entry.getNewValues()[0]).isEqualTo("Bob''s dog");
        assertThat(entry.getNewValues()[1]).isEqualTo("0");

        sql = "UPDATE \"DEBEZIUM\".\"TEST\" SET \"COL2\" = '1' WHERE \"COL1\" = 'Bob''s dog' AND \"COL2\" = '0';";
        entry = fastDmlParser.parse(sql, table, null);
        assertThat(entry.getOperation()).isEqualTo(RowMapper.UPDATE);
        assertThat(entry.getOldValues()).hasSize(2);
        assertThat(entry.getOldValues()[0]).isEqualTo("Bob''s dog");
        assertThat(entry.getOldValues()[1]).isEqualTo("0");
        assertThat(entry.getNewValues()).hasSize(2);
        assertThat(entry.getNewValues()[0]).isEqualTo("Bob''s dog");
        assertThat(entry.getNewValues()[1]).isEqualTo("1");

        sql = "DELETE FROM \"DEBEZIUM\".\"TEST\" WHERE \"COL1\" = 'Bob''s dog' AND \"COL2\" = '1';";
        entry = fastDmlParser.parse(sql, table, null);
        assertThat(entry.getOperation()).isEqualTo(RowMapper.DELETE);
        assertThat(entry.getOldValues()).hasSize(2);
        assertThat(entry.getOldValues()[0]).isEqualTo("Bob''s dog");
        assertThat(entry.getOldValues()[1]).isEqualTo("1");
        assertThat(entry.getNewValues()).isEmpty();
    }

    @Test
    @FixFor("DBZ-3892")
    public void shouldParseConcatenatedUnistrValues() throws Exception {
        final Table table = Table.editor()
                .tableId(TableId.parse("DEBEZIUM.TEST"))
                .addColumn(Column.editor().name("COL1").create())
                .addColumn(Column.editor().name("COL2").create())
                .create();

        // test concatenation in INSERT column values
        String sql = "INSERT INTO \"DEBEZIUM\".\"TEST\"(\"COL1\", \"COL2\") VALUES('1', UNISTR('\0412\044B') || UNISTR('\043F\043E'));";
        LogMinerDmlEntry entry = fastDmlParser.parse(sql, table, null);
        assertThat(entry.getOldValues()).isEmpty();
        assertThat(entry.getNewValues()).hasSize(2);
        assertThat(entry.getNewValues()[0]).isEqualTo("1");
        assertThat(entry.getNewValues()[1]).isEqualTo("UNISTR('\0412\044B') || UNISTR('\043F\043E')");

        // test concatenation in SET statement
        sql = "UPDATE \"DEBEZIUM\".\"TEST\" SET \"COL2\" = UNISTR('\0412\044B') || UNISTR('\043F\043E') WHERE \"COL1\" = '1' AND \"COL2\" IS NULL;";
        entry = fastDmlParser.parse(sql, table, null);
        assertThat(entry.getOldValues()).hasSize(2);
        assertThat(entry.getOldValues()[0]).isEqualTo("1");
        assertThat(entry.getOldValues()[1]).isNull();
        assertThat(entry.getNewValues()).hasSize(2);
        assertThat(entry.getNewValues()[0]).isEqualTo("1");
        assertThat(entry.getNewValues()[1]).isEqualTo("UNISTR('\0412\044B') || UNISTR('\043F\043E')");

        // test concatenation in update WHERE statement
        sql = "UPDATE \"DEBEZIUM\".\"TEST\" SET \"COL2\" = NULL WHERE \"COL1\" = '1' AND \"COL2\" = UNISTR('\0412\044B') || UNISTR('\043F\043E');";
        entry = fastDmlParser.parse(sql, table, null);
        assertThat(entry.getOldValues()).hasSize(2);
        assertThat(entry.getOldValues()[0]).isEqualTo("1");
        assertThat(entry.getOldValues()[1]).isEqualTo("UNISTR('\0412\044B') || UNISTR('\043F\043E')");
        assertThat(entry.getNewValues()).hasSize(2);
        assertThat(entry.getNewValues()[0]).isEqualTo("1");
        assertThat(entry.getNewValues()[1]).isNull();

        // test concatenation in delete WHERE statement
        sql = "DELETE FROM \"DEBEZIUM\".\"TEST\" WHERE \"COL1\" = '1' AND \"COL2\" = UNISTR('\0412\044B') || UNISTR('\043F\043E');";
        entry = fastDmlParser.parse(sql, table, null);
        assertThat(entry.getOldValues()).hasSize(2);
        assertThat(entry.getOldValues()[0]).isEqualTo("1");
        assertThat(entry.getOldValues()[1]).isEqualTo("UNISTR('\0412\044B') || UNISTR('\043F\043E')");
        assertThat(entry.getNewValues()).hasSize(0);
    }
}
