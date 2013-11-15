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

        private string m_commandText;

        private CommandType m_commandType;

        private readonly static Type m_declaringType = typeof(OracleAppender);

		public OracleAppender()
		{
			m_commandType = System.Data.CommandType.Text;
			m_parameters = new List<OracleAppenderParameter>();
		}

        public string ConnectionString
		{
			get { return m_connectionString; }
			set { m_connectionString = value; }
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
                // Oracle transaction start only in the context of a connection. 
                // Because a new connection is created each buffer there is no need for transactions.
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

                            OracleParameter oracleParameter = parameter.Prepare(command);

                            oracleParameter.Value = values;
                        }

                        command.ExecuteNonQuery();

                        // dispose of the OracleParameter(s)?
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

        virtual public OracleParameter Prepare(OracleCommand command)
		{
			OracleParameter parameter = command.CreateParameter();

			parameter.ParameterName = ParameterName;
            parameter.Direction = ParameterDirection.Input;

			if (!m_inferType)
			{
				parameter.DbType = m_dbType;
			}

			if (Precision != 0)
			{
                parameter.Precision = Precision;
			}

			if (Scale != 0)
			{
				parameter.Scale = Scale;
			}

			if (Size != 0)
			{
				parameter.Size = Size;
			}

			command.Parameters.Add(parameter);

            return parameter;
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
