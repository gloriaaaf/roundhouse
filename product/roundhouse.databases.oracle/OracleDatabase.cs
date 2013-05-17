using Oracle.DataAccess.Client;
using roundhouse.infrastructure.logging;

namespace roundhouse.databases.oracle
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Text.RegularExpressions;
    using infrastructure.app;
    using infrastructure.extensions;
    using infrastructure.app.tokens;
    using parameters;

    public sealed class OracleDatabase : AdoNetDatabase
    {
        private string connect_options = "Integrated Security";
        private String db_user = null;
        private String db_psw = null;

        public override string sql_statement_separator_regex_pattern
        {
            get { return @"(?<KEEP1>'{1}[\S\s]*?'{1})|(?<KEEP1>""{1}[\S\s]*?""{1})|(?<KEEP1>(?:\s*)(?:-{2})(?:.*))|(?<KEEP1>/{1}\*{1}[\S\s]*?\*{1}/{1})|(?<KEEP1>(?:\s*)(?:DECLARE{1}[\S\s]*?;\s*?/(?!\*)))|(?<KEEP1>(?:\s*)(?:CREATE\s*OR\s*REPLACE[\S\s]*?;\s*?/(?!\*)))|(?<KEEP1>(?:\s*)(?:BEGIN[\S\s]*?;\s*?/(?!\*)))|(?<KEEP1>\s*)(?<BATCHSPLITTER>;)(?<KEEP2>\s*)"; }
        }

        public override sqlsplitters.StatementSplitter sql_splitter
        {
            get
            {
                return new OracleStatementSplitter(sql_statement_separator_regex_pattern);
            }
        }


        public override bool supports_ddl_transactions
        {
            get { return false; }
        }

        public override void initialize_connections(ConfigurationPropertyHolder configuration_property_holder)
        {
            if (!string.IsNullOrEmpty(connection_string))
            {
                string[] parts = connection_string.Split(';');
                foreach (string part in parts)
                {
                    if (string.IsNullOrEmpty(server_name) && part.to_lower().Contains("data source"))
                    {
                        database_name = part.Substring(part.IndexOf("=") + 1);
                    }

                    if (string.IsNullOrEmpty(server_name) && (part.to_lower().Contains("user id")))
                    {
                        db_user = part.Substring(part.IndexOf("=") + 1);
                    }

                    if (string.IsNullOrEmpty(server_name) && (part.to_lower().Contains("password")))
                    {
                        db_psw = part.Substring(part.IndexOf("=") + 1);
                    }
                }

                if (!connection_string.to_lower().Contains(connect_options.to_lower()))
                {
                    connect_options = string.Empty;
                    foreach (string part in parts)
                    {
                        if (!part.to_lower().Contains("data source"))
                        {
                            connect_options += part + ";";
                        }
                    }
                }
            }
            if (connect_options == "Integrated Security")
            {
                connect_options = "Integrated Security=yes;";
            }

            if (string.IsNullOrEmpty(connection_string))
            {
                connection_string = build_connection_string(database_name, connect_options);
            }

            configuration_property_holder.ConnectionString = connection_string;

            set_provider();
            if (string.IsNullOrEmpty(admin_connection_string))
            {
                admin_connection_string = Regex.Replace(connection_string, "Integrated Security=.*?;", "Integrated Security=yes;");
                admin_connection_string = Regex.Replace(admin_connection_string, "User Id=.*?;", string.Empty);
                admin_connection_string = Regex.Replace(admin_connection_string, "Password=.*?;", string.Empty);
            }
            configuration_property_holder.ConnectionStringAdmin = admin_connection_string;
        }

        private static string build_connection_string(string database_name, string connection_options)
        {
            return string.Format("Data Source={0};{1}", database_name, connection_options);
        }

        public override void set_provider()
        {
            provider = "Oracle.DataAccess.Client";
        }

        protected override void connection_specific_setup(IDbConnection connection)
        {
            ((OracleConnection)connection).InfoMessage += (sender, e) => Log.bound_to(this).log_a_debug_event_containing("  [SQL PRINT]: {0}{1}", Environment.NewLine, e.Message);
        }

        public override void create_or_update_roundhouse_tables()
        {
            Log.bound_to(this).log_an_info_event_containing("Creating table [{0}_{1}].", roundhouse_schema_name, version_table_name);
            run_sql(create_roundhouse_version_table(version_table_name), ConnectionType.Default);
            Log.bound_to(this).log_an_info_event_containing("Creating table [{0}_{1}].", roundhouse_schema_name, scripts_run_table_name);
            run_sql(create_roundhouse_scripts_run_table(scripts_run_table_name),ConnectionType.Default);
            Log.bound_to(this).log_an_info_event_containing("Creating table [{0}_{1}].", roundhouse_schema_name, scripts_run_errors_table_name);
            run_sql(create_roundhouse_scripts_run_errors_table(scripts_run_errors_table_name),ConnectionType.Default);
        }


        public override void run_database_specific_tasks()
        {
            Log.bound_to(this).log_an_info_event_containing("Creating a sequence for the '{0}' table.", version_table_name);
            run_sql(create_sequence_script(version_table_name), ConnectionType.Default);
            Log.bound_to(this).log_an_info_event_containing("Creating a sequence for the '{0}' table.", scripts_run_table_name);
            run_sql(create_sequence_script(scripts_run_table_name), ConnectionType.Default);
            Log.bound_to(this).log_an_info_event_containing("Creating a sequence for the '{0}' table.", scripts_run_errors_table_name);
            run_sql(create_sequence_script(scripts_run_errors_table_name), ConnectionType.Default);
        }

       public override string get_version(string repository_path)
        {
            return run_sql_scalar(get_version_script(repository_path), ConnectionType.Default, null) as string;
        }


        public override long insert_version_and_get_version_id(string repository_path, string repository_version)
        {
            var insert_parameters = new List<IParameter<IDbDataParameter>>
                                        {
                                            create_parameter("repository_path", DbType.AnsiString, repository_path, 255),
                                            create_parameter("repository_version", DbType.AnsiString, repository_version, 35),
                                            create_parameter("user_name", DbType.AnsiString, user_name, 50)
                                        };
            run_sql(insert_version_script(), ConnectionType.Default, insert_parameters);

            var select_parameters = new List<IParameter<IDbDataParameter>> { create_parameter("repository_path", DbType.AnsiString, repository_path, 255) };
            return Convert.ToInt64(run_sql_scalar(get_version_id_script(), ConnectionType.Default, select_parameters));
        }

        public override void run_sql(string sql_to_run, ConnectionType connection_type)
        {
            Log.bound_to(this).log_a_debug_event_containing("Replacing script text \r\n with \n to be compliant with Oracle.");
            // http://www.barrydobson.com/2009/02/17/pls-00103-encountered-the-symbol-when-expecting-one-of-the-following/
            base.run_sql(sql_to_run.Replace("\r\n", "\n").Replace("\r", "\n"), connection_type);
        }

        protected override object run_sql_scalar(string sql_to_run, ConnectionType connection_type, IList<IParameter<IDbDataParameter>> parameters)
        {
            Log.bound_to(this).log_a_debug_event_containing("Replacing \r\n with \n to be compliant with Oracle.");
            //http://www.barrydobson.com/2009/02/17/pls-00103-encountered-the-symbol-when-expecting-one-of-the-following/
            sql_to_run = sql_to_run.Replace("\r\n", "\n");
            object return_value = new object();

            if (string.IsNullOrEmpty(sql_to_run)) return return_value;

            using (IDbCommand command = setup_database_command(sql_to_run, connection_type, parameters))
            {
                return_value = command.ExecuteScalar();
                command.Dispose();
            }

            return return_value;
        }

        /// <summary>
        /// This DOES NOT use the ADMIN connection. Use sparingly.
        /// </summary>
        private IParameter<IDbDataParameter> create_parameter(string name, DbType type, object value, int? size)
        {
            IDbCommand command = server_connection.underlying_type().CreateCommand();
            var parameter = command.CreateParameter();
            command.Dispose();

            parameter.Direction = ParameterDirection.Input;
            parameter.ParameterName = name;
            parameter.DbType = type;
            parameter.Value = value ?? DBNull.Value;
            if (size != null)
            {
                parameter.Size = size.Value;
            }

            return new AdoNetParameter(parameter);
        }

        public override bool create_database_if_it_doesnt_exist(string custom_create_database_script)
        {
            bool database_was_created = false;
            try
            {
                string create_script = create_database_script();
                if (!string.IsNullOrEmpty(custom_create_database_script))
                {
                    create_script = custom_create_database_script;
                    if (!configuration.DisableTokenReplacement)
                    {
                        create_script = TokenReplacer.replace_tokens(configuration, create_script);
                    }
                }

                if (split_batch_statements)
                {
                    foreach (var sql_statement in sql_splitter.split(create_script))
                    {
                        //should only receive a return value once
                        var return_value = run_sql_scalar_boolean(sql_statement, ConnectionType.Admin);
                        if (return_value != null)
                        {
                            database_was_created = return_value.Value;
                        }
                    }
                }
                else
                {
                    //should only receive a return value once
                    var return_value = run_sql_scalar_boolean(create_script, ConnectionType.Admin);
                    database_was_created = return_value.GetValueOrDefault(false);
                }
            }
            catch (Exception ex)
            {
                Log.bound_to(this).log_a_warning_event_containing(
                    "{0} with provider {1} does not provide a facility for creating a database at this time.{2}{3}",
                    GetType(), provider, Environment.NewLine, ex.Message);
            }

            return database_was_created;
        }

        private bool? run_sql_scalar_boolean(string sql_to_run, ConnectionType connection_type)
        {
            var return_value = run_sql_scalar(sql_to_run, connection_type, null);
            if (return_value != null && return_value != DBNull.Value)
            {
                return Convert.ToBoolean(return_value);
            }
            return null;
        }

        public override string set_recovery_mode_script(bool simple)
        {
            return string.Empty;
        }

        public override string restore_database_script(string restore_from_path, string custom_restore_options)
        {
            return string.Empty;
        }

        public override string create_database_script()
        {
            return string.Format(
           @"
                DECLARE
                    v_exists Integer := 0;
                BEGIN
                    SELECT COUNT(*) INTO v_exists FROM dba_users WHERE username = '{0}';
                    IF v_exists = 0 THEN
                        EXECUTE IMMEDIATE 'CREATE USER {0} IDENTIFIED BY {1}';
                        EXECUTE IMMEDIATE 'GRANT DBA TO {0}';
                        EXECUTE IMMEDIATE 'ALTER USER {0}  DEFAULT ROLE DBA';                            
                    END IF;
                END;
                /                        
                ", db_user.ToUpper(), db_psw);
        }

        public string create_roundhouse_version_table(string table_name)
        {
            return string.Format(
                @"
                    DECLARE
                        tableExists Integer := 0;
                    BEGIN
                        SELECT COUNT(*) INTO tableExists FROM user_objects WHERE object_type = 'TABLE' AND UPPER(object_name) = UPPER('{1}_{2}');
                        IF tableExists = 0 THEN   
                        
                            EXECUTE IMMEDIATE 'CREATE TABLE {0}.{1}_{2} (
                            ID NUMBER(20,0) NOT NULL ENABLE,
	                        REPOSITORY_PATH NVARCHAR2(255),
	                        VERSION NVARCHAR2(50),
	                        ENTRY_DATE TIMESTAMP (4),
	                        MODIFIED_DATE TIMESTAMP (4),
	                        ENTERED_BY NVARCHAR2(50),
	                        PRIMARY KEY (ID) )';
                            
                        END IF;
                    END;
              ",
               db_user, roundhouse_schema_name, table_name);

        }

        public string create_roundhouse_scripts_run_table(string table_name)
        {
            return string.Format(
                @"
                    DECLARE
                        tableExists Integer := 0;
                    BEGIN
                        SELECT COUNT(*) INTO tableExists FROM user_objects WHERE object_type = 'TABLE' AND UPPER(object_name) = UPPER('{1}_{2}');
                        IF tableExists = 0 THEN   
                        
                            EXECUTE IMMEDIATE 'CREATE TABLE {0}.{1}_{2} (
                            ID NUMBER(20,0) NOT NULL ENABLE,
	                        VERSION_ID NUMBER(20,0),
	                        SCRIPT_NAME NVARCHAR2(255),
	                        TEXT_OF_SCRIPT CLOB,
	                        TEXT_HASH NVARCHAR2(512),
	                        ONE_TIME_SCRIPT NUMBER(1,0),
	                        ENTRY_DATE TIMESTAMP (4),
	                        MODIFIED_DATE TIMESTAMP (4),
	                        ENTERED_BY NVARCHAR2(50),
	                        PRIMARY KEY (ID) )';
                            
                        END IF;
                    END;
              ",
               db_user, roundhouse_schema_name, table_name);

        }

        public string create_roundhouse_scripts_run_errors_table(string table_name)
        {
            return string.Format(
                @"
                    DECLARE
                        tableExists Integer := 0;
                    BEGIN
                        SELECT COUNT(*) INTO tableExists FROM user_objects WHERE object_type = 'TABLE' AND UPPER(object_name) = UPPER('{1}_{2}');
                        IF tableExists = 0 THEN   
                        
                            EXECUTE IMMEDIATE 'CREATE TABLE {0}.{1}_{2} (
                            ID NUMBER(20,0) NOT NULL ENABLE,
	                        REPOSITORY_PATH NVARCHAR2(255),
	                        VERSION NVARCHAR2(50),
	                        SCRIPT_NAME NVARCHAR2(255),
	                        TEXT_OF_SCRIPT CLOB,
	                        ERRONEOUS_PART_OF_SCRIPT CLOB,
	                        ERROR_MESSAGE CLOB,
	                        ENTRY_DATE TIMESTAMP (4),
	                        MODIFIED_DATE TIMESTAMP (4),
	                        ENTERED_BY NVARCHAR2(50),
	                        PRIMARY KEY (ID) )';
                            
                        END IF;
                    END;
              ",
               db_user, roundhouse_schema_name, table_name);

        }


        public string create_sequence_script(string table_name)
        {
            return string.Format(
                @"
                    DECLARE
                        sequenceExists Integer := 0;
                    BEGIN
                        SELECT COUNT(*) INTO sequenceExists FROM user_objects WHERE object_type = 'SEQUENCE' AND UPPER(object_name) = UPPER('{1}_{2}ID');
                        IF sequenceExists = 0 THEN   
                        
                            EXECUTE IMMEDIATE 'CREATE SEQUENCE {0}.{1}_{2}id
                            START WITH 1
                            INCREMENT BY 1
                            MINVALUE 1
                            MAXVALUE 999999999999999999999999999
                            CACHE 20
                            NOCYCLE 
                            NOORDER';
                            
                        END IF;
                    END;
              ",
               db_user, roundhouse_schema_name, table_name);
        }

        public string insert_version_script()
        {
            return string.Format(
                @"
                    INSERT INTO {0}.{1}_{2}
                    (
                        id
                        ,repository_path
                        ,version
                        ,entered_by
                    )
                    VALUES
                    (
                        {0}.{1}_{2}id.NEXTVAL
                        ,:repository_path
                        ,:repository_version
                        ,:user_name
                    )
                ",
                db_user,roundhouse_schema_name, version_table_name);
        }

        public string get_version_script(string repository_path)
        {
            return string.Format(
                 @"
                    SELECT version
                    FROM (SELECT * FROM {0}.{1}_{2}
                            WHERE 
                                repository_path = '{3}'
                            ORDER BY entry_date DESC)
                    WHERE ROWNUM < 2
                ",
                db_user,roundhouse_schema_name, version_table_name, repository_path);
        }


        public string get_version_id_script()
        {
            return string.Format(
                @"
                    SELECT id
                    FROM (SELECT * FROM {0}.{1}_{2}
                            WHERE 
                                NVL(repository_path, '') = NVL(:repository_path, '')
                            ORDER BY entry_date DESC)
                    WHERE ROWNUM < 2
                ", 
                 db_user,roundhouse_schema_name, version_table_name);
        }

        public override string delete_database_script()
        {
            return string.Format(
            @" 
                DECLARE
                    v_exists Integer := 0;
                BEGIN
                    SELECT COUNT(*) INTO v_exists FROM dba_users WHERE username = '{0}';
                    IF v_exists > 0 THEN
                        EXECUTE IMMEDIATE 'DROP USER {0} CASCADE';
                    END IF;
                END;
                /",
            database_name.to_upper());
        }

    }
}