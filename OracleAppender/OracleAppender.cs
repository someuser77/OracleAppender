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
using log4net.Util;

namespace CustomLog4netAppender
{
    public class OracleAppender : BufferingAppenderSkeleton
	{
        protected List<OracleAppenderParameter> m_parameters;

        private SecurityContext m_securityContext;

        private string m_connectionString;

        private string m_appSettingsKey;

        private string m_connectionStringName;

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

			if (m_securityContext == null)
			{
				m_securityContext = SecurityContextProvider.DefaultProvider.CreateSecurityContext(this);
			}
		}

		override protected void SendBuffer(LoggingEvent[] events)
		{
            try
            {
                using (OracleConnection connection = new OracleConnection(ConnectionString))
                {
                    connection.Open();

                    using (OracleCommand command = connection.CreateCommand())
                    {
                        command.BindByName = true;
                        command.CommandType = CommandType;
                        command.CommandText = CommandText;
                        command.ArrayBindCount = events.Length;

                        foreach (OracleAppenderParameter parameter in m_parameters)
                        {
                            object[] values = new object[events.Length];

                            for (int i = 0; i < values.Length; i++)
                            {
                                LoggingEvent loggingEvent = events[i];

                                object formatterValue = parameter.Layout.Format(loggingEvent);

                                if (formatterValue == null)
                                {
                                    formatterValue = DBNull.Value;
                                }

                                values[i] = formatterValue;
                            }

                            OracleParameter oracleParameter = command.CreateParameter();
                            oracleParameter.DbType = parameter.DbType;
                            oracleParameter.ParameterName = parameter.ParameterName;
                            oracleParameter.Precision = parameter.Precision;
                            oracleParameter.Scale = parameter.Scale;
                            oracleParameter.Direction = ParameterDirection.Input;

                            oracleParameter.Value = values;

                            command.Parameters.Add(oracleParameter);
                        }

                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }
            }
            catch (Exception e)
            {
                ErrorHandler.Error("OracleAppender error.", e);
                throw;
            }
		}

        public void AddParameter(OracleAppenderParameter parameter)
        {
            m_parameters.Add(parameter);
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
	}

	public class OracleAppenderParameter
	{
        private DbType m_dbType;
        private bool m_inferType;

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
