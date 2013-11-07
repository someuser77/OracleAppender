using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using log4net.Appender;
using Oracle.DataAccess.Client;
using System.Configuration;
using log4net.Core;
using System.Data;
using log4net.Layout;

namespace CustomLog4netAppender
{
    public class OracleAppender : BufferingAppenderSkeleton
	{
        protected bool m_usePreparedCommand;

        protected List<OracleAppenderParameter> m_parameters;

        private SecurityContext m_securityContext;

        private IDbConnection m_dbConnection;

        private IDbCommand m_dbCommand;

        private string m_connectionString;

        private string m_appSettingsKey;

        private string m_connectionStringName;

        private string m_connectionType;

        private string m_commandText;

        private CommandType m_commandType;

        private bool m_useTransactions;

        private readonly static Type m_declaringType = typeof(OracleAppender);

		public OracleAppender()
		{
			m_useTransactions = false;
			m_commandType = System.Data.CommandType.Text;
			m_parameters = new List<OracleAppenderParameter>();
		}

        public string ConnectionString
		{
			get { return m_connectionString; }
			set { m_connectionString = value; }
		}

	    public string AppSettingsKey
	    {
	        get { return m_appSettingsKey; }
	        set { m_appSettingsKey = value; }
	    }

	    public string ConnectionStringName
	    {
	        get { return m_connectionStringName; }
	        set { m_connectionStringName = value; }
	    }
		
        public string CommandText
		{
			get { return m_commandText; }
			set { m_commandText = value; }
		}

		public CommandType CommandType
		{
			get { return m_commandType; }
			set { m_commandType = value; }
		}
		
        public bool UseTransactions
		{
			get { return m_useTransactions; }
			set { m_useTransactions = value; }
		}

		public SecurityContext SecurityContext 
		{
			get { return m_securityContext; }
			set { m_securityContext = value; }
		}

		override public void ActivateOptions() 
		{
			base.ActivateOptions();

			// Are we using a command object
			m_usePreparedCommand = (m_commandText != null && m_commandText.Length > 0);

			if (m_securityContext == null)
			{
				m_securityContext = SecurityContextProvider.DefaultProvider.CreateSecurityContext(this);
			}
		}

		override protected void SendBuffer(LoggingEvent[] events)
		{
            using (OracleConnection connection = new OracleConnection(ConnectionString))
            {
                
                        foreach (OracleAppenderParameter parameter in m_parameters)
                        {
                            object[] values = new object[events.Length];

                            for (int i=0;i<values.Length;i++)
                            {
                                LoggingEvent loggingEvent = events[i];

                                object formatterValue = parameter.Layout.Format(loggingEvent);

                                if (formatterValue == null)
                                {
                                    formatterValue = DBNull.Value;
                                }

                                values[i] = formatterValue;
                            }








                using (OracleCommand command = connection.CreateCommand())
                {
                    command.BindByName = true;
                    command.CommandType = CommandType;
                    command.CommandText = CommandText;
                    command.ArrayBindCount = events.Length;

                    foreach (LoggingEvent loggingEvent in events)
                    {
                        foreach (OracleAppenderParameter parameters in m_parameters)
                        {
                            parameters.FormatValue(command, loggingEvent);
                        }

                        // Execute the query
                        m_dbCommand.ExecuteNonQuery();
                    }
                            
                }
            }
		}

        public void AddParameter(OracleAppenderParameter parameter)
        {
            m_parameters.Add(parameter);
        }

		virtual protected string GetLogStatement(LoggingEvent logEvent)
		{
			if (Layout == null)
			{
				ErrorHandler.Error("AdoNetAppender: No Layout specified.");
				return "";
			}
			else
			{
				StringWriter writer = new StringWriter(System.Globalization.CultureInfo.InvariantCulture);
				Layout.Format(writer, logEvent);
				return writer.ToString();
			}
		}
        
        virtual protected IDbConnection CreateConnection(Type connectionType, string connectionString)
        {
            IDbConnection connection = (IDbConnection)Activator.CreateInstance(connectionType);
            connection.ConnectionString = connectionString;
            return connection;
        }

        virtual protected string ResolveConnectionString(out string connectionStringContext)
        {
            if (m_connectionString != null && m_connectionString.Length > 0)
            {
                connectionStringContext = "ConnectionString";
                return m_connectionString;
            }

            if (!String.IsNullOrEmpty(m_connectionStringName))
            {
                ConnectionStringSettings settings = ConfigurationManager.ConnectionStrings[m_connectionStringName];
                if (settings != null)
                {
                    connectionStringContext = "ConnectionStringName";
                    return settings.ConnectionString;
                }
                else
                {
                    throw new LogException("Unable to find [" + m_connectionStringName + "] ConfigurationManager.ConnectionStrings item");
                }
            }

            if (m_appSettingsKey != null && m_appSettingsKey.Length > 0)
            {
                connectionStringContext = "AppSettingsKey";
                string appSettingsConnectionString = SystemInfo.GetAppSetting(m_appSettingsKey);
                if (appSettingsConnectionString == null || appSettingsConnectionString.Length == 0)
                {
                    throw new LogException("Unable to find [" + m_appSettingsKey + "] AppSettings key.");
                }
                return appSettingsConnectionString;
            }

            connectionStringContext = "Unable to resolve connection string from ConnectionString, ConnectionStrings, or AppSettings.";
            return string.Empty;
        }

        private void InitializeDatabaseCommand()
        {
            if (m_dbConnection != null && m_usePreparedCommand)
            {
                try
                {
                    DisposeCommand(false);

                    // Create the command object
                    m_dbCommand = m_dbConnection.CreateCommand();

                    // Set the command string
                    m_dbCommand.CommandText = m_commandText;

                    // Set the command type
                    m_dbCommand.CommandType = m_commandType;
                }
                catch (Exception e)
                {
                    ErrorHandler.Error("Could not create database command [" + m_commandText + "]", e);

                    DisposeCommand(true);
                }

                if (m_dbCommand != null)
                {
                    try
                    {
                        foreach (OracleAppenderParameter param in m_parameters)
                        {
                            try
                            {
                                param.Prepare(m_dbCommand);
                            }
                            catch (Exception e)
                            {
                                ErrorHandler.Error("Could not add database command parameter [" + param.ParameterName + "]", e);
                                throw;
                            }
                        }
                    }
                    catch
                    {
                        DisposeCommand(true);
                    }
                }

                if (m_dbCommand != null)
                {
                    try
                    {
                        // Prepare the command statement.
                        m_dbCommand.Prepare();
                    }
                    catch (Exception e)
                    {
                        ErrorHandler.Error("Could not prepare database command [" + m_commandText + "]", e);

                        DisposeCommand(true);
                    }
                }
            }
        }
	}

	public class OracleAppenderParameter
	{
        private DbType m_dbType;

        private bool m_inferType;

        private IRawLayout m_layout;

        public OracleAppenderParameter()
        {
            m_inferType = true;
            Precision = 0;
            Scale = 0;
            Size = 0;
        }

        public string ParameterName { get; set; }

		public DbType DbType
		{
			get { return m_dbType; }
			set 
			{ 
				m_dbType = value; 
				m_inferType = false;
			}
		}

        public byte Precision { get; set; }

        public byte Scale { get; set; }

        public int Size { get; set; }

        public IRawLayout Layout { get; set; }

		virtual public void Prepare(OracleCommand command)
		{
			OracleParameter param = command.CreateParameter();

			param.ParameterName = ParameterName;

			if (!m_inferType)
			{
				param.DbType = m_dbType;
			}

			if (Precision != 0)
			{
                param.Precision = Precision;
			}

			if (Scale != 0)
			{
				param.Scale = Scale;
			}

			if (Size != 0)
			{
				param.Size = Size;
			}

			command.Parameters.Add(param);
		}

		virtual public void FormatValue(OracleCommand command, LoggingEvent loggingEvent)
		{
			OracleParameter param = command.Parameters[ParameterName];

			object formattedValue = Layout.Format(loggingEvent);

			if (formattedValue == null)
			{
				formattedValue = DBNull.Value;
			}

			param.Value = formattedValue;
		}


	}
}
