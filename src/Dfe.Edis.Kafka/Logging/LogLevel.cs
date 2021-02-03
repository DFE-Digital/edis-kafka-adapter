namespace Dfe.Edis.Kafka.Logging
{
    public enum LogLevel
    {
        /// <summary>System is unusable.</summary>
        Emergency,
        /// <summary>Action must be take immediately</summary>
        Alert,
        /// <summary>Critical condition.</summary>
        Critical,
        /// <summary>Error condition.</summary>
        Error,
        /// <summary>Warning condition.</summary>
        Warning,
        /// <summary>Normal, but significant condition.</summary>
        Notice,
        /// <summary>Informational message.</summary>
        Info,
        /// <summary>Debug-level message.</summary>
        Debug,
    }
}