        SQLMergeChangeLogStatement stmt = new SQLMergeChangeLogStatement();
        boolean isMergingDatabase = fileType.equals(FileType.DDL_CHANGE_LOG);
        stmt.setDbType(dbType);
        stmt.setTargetIsDatabase(isMergingDatabase);
        lexer.nextToken();
        SQLExpr targetNameExpr = exprParser.expr();
        if (!(targetNameExpr instanceof SQLName)) {
            throw new ParserException(
                    "target name of MERGE INTO " + (isMergingDatabase ? "DATABASE" : "TABLE") + " is not valid name expression. " + lexer.info());
        }