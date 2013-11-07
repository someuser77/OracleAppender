using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using log4net;
using log4net.Config;

namespace OracleLogging
{
    class Program
    {
        static void Main(string[] args)
        {
            ILog log = LogManager.GetLogger(typeof(Program));
            XmlConfigurator.Configure();
            log.Debug("Message.");
        }
    }
}
