using roundhouse.sqlsplitters;

namespace roundhouse.databases.oracle
{
    using System.Collections.Generic;
    using System.Text.RegularExpressions;
    using infrastructure.extensions;

    public class OracleStatementSplitter : StatementSplitter
    {
        public OracleStatementSplitter(string sql_statement_separator_regex_pattern)
        {
            this.sql_statement_separator_regex_pattern = sql_statement_separator_regex_pattern;
        }

        private string sql_statement_separator_regex_pattern;

        private static string batch_terminator_replacement_string = @" |{[_REMOVE_]}| ";
        private static string regex_split_string = @"\|\{\[_REMOVE_\]\}\|";
        public static string statement_separator_regex_create_block = @"(?<KEEP1>'{1}[\S\s]*?'{1})|(?<KEEP1>""{1}[\S\s]*?""{1})|(?<KEEP1>(?:\s*)(?:-{2})(?:.*))|(?<KEEP1>/{1}\*{1}[\S\s]*?\*{1}/{1})|(?<KEEP1>(?:\s*DECLARE{1}[\S\s]*?;\s*?/(?!\*)))|(?<KEEP1>\s*)(?<BATCHSPLITTER>(?:\s*CREATE\s*OR\s*REPLACE[\S\s]*?;\s*?/(?!\*)))(?<KEEP2>\s*)";
        public static string statement_separator_regex_declare = @"(?<KEEP1>(?:\s*CREATE\s*OR\s*REPLACE[\S\s]*?\|\{))|(?<KEEP1>\s*)(?<BATCHSPLITTER>(?:\s*DECLARE{1}[\S\s]*?;\s*?/(?!\*)))(?<KEEP2>\s*)";
        public static string statement_separator_regex_begin_block = @"(?<KEEP1>(?:\s*DECLARE{1}[\S\s]*?\|\{))|(?<KEEP1>(?:\s*CREATE\s*OR\s*REPLACE[\S\s]*?\|\{))|(?<KEEP1>\s*)(?<BATCHSPLITTER>(?:\s*BEGIN[\S\s]*?;\s*?/(?!\*)))(?<KEEP2>\s*)";
        public static string statement_separator_regex_view = @"(?:CREATE\s*OR\s*REPLACE[\S\s]*?VIEW[\S\s]*?;\s*?/(?!\*))";
        public static string statement_separator_regex_synonym = @"(?:CREATE\s*OR\s*REPLACE[\S\s]*?SYNONYM[\S\s]*?;\s*?/(?!\*))";
        public static string regex_end_block = @"(;[\s]*?/(?!\*))";
        public static string regex_comments = @"(:?\s*-{2}.*)|(:?\s*/{1}\*{1}[\S\s]*?\*{1}/{1})";


        public IEnumerable<string> split(string sql_to_run)
        {
            IList<string> return_sql_list = new List<string>();

            Regex regex_replace = new Regex(sql_statement_separator_regex_pattern, RegexOptions.IgnoreCase | RegexOptions.Multiline);

            string sql_statement_scrubbed = regex_replace.Replace(sql_to_run, match => evaluate_and_replace_batch_split_items(match, regex_replace));

            //Oracle create or replace block

            regex_replace = new Regex(statement_separator_regex_create_block, RegexOptions.IgnoreCase | RegexOptions.Multiline);

            sql_statement_scrubbed = regex_replace.Replace(sql_statement_scrubbed, match => evaluate_and_replace_batch_blocks(match, regex_replace));

            //Oracle declare block

            regex_replace = new Regex(statement_separator_regex_declare, RegexOptions.IgnoreCase | RegexOptions.Multiline);

            sql_statement_scrubbed = regex_replace.Replace(sql_statement_scrubbed, match => evaluate_and_replace_batch_blocks(match, regex_replace));

            //Oracle begin block

            regex_replace = new Regex(statement_separator_regex_begin_block, RegexOptions.IgnoreCase | RegexOptions.Multiline);

            sql_statement_scrubbed = regex_replace.Replace(sql_statement_scrubbed, match => evaluate_and_replace_batch_blocks(match, regex_replace));


            foreach (string sql_statement in Regex.Split(sql_statement_scrubbed, regex_split_string, RegexOptions.IgnoreCase | RegexOptions.Multiline))
            {
                if (script_has_text_to_run(sql_statement, sql_statement_separator_regex_pattern))
                {
                    return_sql_list.Add(sql_statement.Trim());
                }
            }

            return return_sql_list;
        }

        private static string evaluate_and_replace_batch_split_items(Match matched_item, Regex regex)
        {
            if (matched_item.Groups["BATCHSPLITTER"].Success)
            {
                return matched_item.Groups["KEEP1"].Value + batch_terminator_replacement_string + matched_item.Groups["KEEP2"].Value;
            }
            else
            {
                return matched_item.Groups["KEEP1"].Value + matched_item.Groups["KEEP2"].Value;
            }
        }

        public static string evaluate_and_replace_batch_blocks(Match matched_item, Regex regex)
        {
            if (matched_item.Groups["BATCHSPLITTER"].Success)
            {
                Regex regex_view = new Regex(statement_separator_regex_view, RegexOptions.IgnoreCase | RegexOptions.Multiline);
                Regex regex_synonym = new Regex(statement_separator_regex_synonym, RegexOptions.IgnoreCase | RegexOptions.Multiline);
                //Views blocks don't need ; at he end of the statement otherwise throw invalid character error
                if (regex_view.IsMatch(matched_item.Groups["BATCHSPLITTER"].Value) || regex_synonym.IsMatch(matched_item.Groups["BATCHSPLITTER"].Value))
                {
                    return Regex.Replace(matched_item.Groups["BATCHSPLITTER"].Value, regex_end_block, batch_terminator_replacement_string, RegexOptions.IgnoreCase | RegexOptions.Multiline);
                }
                else
                {
                    return Regex.Replace(matched_item.Groups["BATCHSPLITTER"].Value, regex_end_block, ";" + batch_terminator_replacement_string, RegexOptions.IgnoreCase | RegexOptions.Multiline);
                }
            }
            else
            {
                return matched_item.Groups["KEEP1"].Value + matched_item.Groups["KEEP2"].Value;
            }
        }


        private static bool script_has_text_to_run(string sql_statement, string sql_statement_separator_regex_pattern)
        {
            sql_statement = sql_statement.Replace("\r\n", "\n");
            sql_statement = sql_statement.Replace("\r", "\n");
            string aux = Regex.Replace(sql_statement, regex_comments, "", RegexOptions.IgnoreCase | RegexOptions.Multiline).Trim();

            if (aux.Length > 0)
            {
                aux = Regex.Replace(sql_statement, regex_split_string, string.Empty, RegexOptions.IgnoreCase | RegexOptions.Multiline);
            }

            return !string.IsNullOrEmpty(aux.to_lower().Replace(System.Environment.NewLine, string.Empty).Replace(" ", string.Empty));
        }
    }
}
