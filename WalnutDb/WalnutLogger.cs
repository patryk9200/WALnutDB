using System.Runtime.CompilerServices;

namespace WalnutDb
{
    public static class WalnutLogger
    {
        public static bool Debug = false;
        public delegate void WarningHandler(string callerInfo, string message);
        public static event WarningHandler? OnWarning;

        public delegate void DebugHandler(string callerInfo, string message);
        public static event DebugHandler? OnDebug;

        public delegate void ExceptionHandler(string callerInfo, Exception exception);
        public static event ExceptionHandler? OnException;

        public static void Warning(string message, [CallerMemberName] string caller = "", [CallerLineNumber] int callerLine = 0, [CallerFilePath] string callerFilePath = "")
        {
            if (!Debug)
                return;

            var callerInfo = Debug ? $"{caller}>{callerFilePath}:{callerLine}" : caller;
            OnWarning?.Invoke(callerInfo, message);
        }

        public static void DebugLog(string message, [CallerMemberName] string caller = "", [CallerLineNumber] int callerLine = 0, [CallerFilePath] string callerFilePath = "")
        {
            if (!Debug)
                return;

            var callerInfo = $"{caller}>{callerFilePath}:{callerLine}";
            OnDebug?.Invoke(callerInfo, message);
        }

        public static void Exception(Exception exception, [CallerMemberName] string caller = "", [CallerLineNumber] int callerLine = 0, [CallerFilePath] string callerFilePath = "")
        {
            if (!Debug)
                return;

            var callerInfo = $"{caller}>{callerFilePath}:{callerLine}";
            OnException?.Invoke(callerInfo, exception);
        }
    }
}
