using System;
using System.Collections.Concurrent;
using System.Management.Automation;
using System.Management.Automation.Runspaces;
using System.Threading;
using System.Threading.Tasks;

namespace DurableFunction
{
    public class Program
    {
        static void Main(string[] args)
        {
            var app = new Program();
            app.HandleEvents();
        }

        private const string ScriptPath = @"F:\tmp\temp\durable.ps1";
        private readonly PowerShell powershell;
        private readonly AutoResetEvent waitHandle;
        private readonly ConcurrentDictionary<string, string> activityFuncResults;
        private readonly BlockingCollection<ActivityEvent> eventQueue;

        internal Program()
        {
            powershell = PowerShell.Create(InitialSessionState.CreateDefault());
            InitializePowerShell(powershell);
            eventQueue = new BlockingCollection<ActivityEvent>(boundedCapacity: 3);
            eventQueue.TryAdd(new ActivityEvent(EventType.StartExecution));
            waitHandle = new AutoResetEvent(initialState: false);
            activityFuncResults = new ConcurrentDictionary<string, string>();
        }

        private void InitializePowerShell(PowerShell powershell)
        {
            powershell.AddCommand("Set-ExecutionPolicy").AddParameter("ExecutionPolicy", "Unrestricted").AddParameter("Scope", "Process").Invoke();
            powershell.Commands.Clear();
            powershell.AddCommand("Import-Module").AddParameter("Assembly", typeof(Program).Assembly).Invoke();
            powershell.Commands.Clear();
            powershell.Streams.ClearStreams();
        }

        internal void RunDurableFunction()
        {
            Console.WriteLine($"Start running the orchestration function ...");
            var context = new OrchestrationContext(waitHandle, activityFuncResults, eventQueue);
            powershell.AddCommand(ScriptPath, useLocalScope: true).AddParameter("Context", context);
            
            var outputBuffer = new PSDataCollection<PSObject>();
            var asyncResult = powershell.BeginInvoke<object, PSObject>(input: null, output: outputBuffer);

            var waitHandles = new WaitHandle[] { waitHandle, asyncResult.AsyncWaitHandle };
            int index = WaitHandle.WaitAny(waitHandles);

            if (index == 0)
            {
                powershell.Stop();
                powershell.Streams.ClearStreams();
            }
            else
            {
                powershell.EndInvoke(asyncResult);
                Console.WriteLine($"Orchestration function finished running");
                Console.WriteLine($"Result: {outputBuffer[0].ToString()}");
            }

            powershell.Commands.Clear();
        }

        internal void HandleEvents()
        {
            while (!eventQueue.IsCompleted)
            {
                Console.WriteLine("Reading an event ...");
                var @event = eventQueue.Take();
                Console.WriteLine($"Got an event back. Type: {@event.EventType}");
                switch (@event.EventType)
                {
                    case EventType.StartExecution:
                        RunDurableFunction();
                        break;
                    default:
                        break;
                }
            }
        }
    }

    internal class ActivityEvent
    {
        internal ActivityEvent(EventType type)
        {
            EventType = type;
        }

        internal readonly EventType EventType;
    }

    internal enum EventType
    {
        StartExecution
    }

    public class OrchestrationContext
    {
        internal OrchestrationContext(
            EventWaitHandle handler,
            ConcurrentDictionary<string, string> results,
            BlockingCollection<ActivityEvent> events)
        {
            Handler = handler;
            Results = results;
            Events = events;
        }

        internal readonly EventWaitHandle Handler;
        internal readonly ConcurrentDictionary<string, string> Results;
        internal readonly BlockingCollection<ActivityEvent> Events;
    }

    [Cmdlet("Invoke", "ActivityFunction")]
    public class InvokeActivityFunctionCommand : PSCmdlet
    {
        [Parameter(Mandatory = true)]
        public string FunctionName;

        [Parameter]
        public string InputValue;

        [Parameter(Mandatory = true)]
        public OrchestrationContext Context;

        private ManualResetEvent waitHandler = new ManualResetEvent(initialState: false);

        protected override void ProcessRecord()
        {
            if (Context.Results.TryGetValue(FunctionName, out string value))
            {
                Console.WriteLine($"    Replay 'Invoke-ActivityFunction -FunctionName {FunctionName}'");
                Console.WriteLine($"        Result for {FunctionName} is ready: {value}");
                WriteObject(value);
            }
            else
            {
                Console.WriteLine($"    Schedule activity function '{FunctionName}'.");
                Task.Delay(2000).ContinueWith(ScheduleActivityFunction);
                Context.Handler.Set();
                waitHandler.WaitOne();
            }
        }

        protected override void StopProcessing()
        {
            waitHandler.Set();
        }

        internal void ScheduleActivityFunction(Task task)
        {
            Context.Results.TryAdd(FunctionName, $"{FunctionName}-Input-{InputValue ?? "N/A"}-COMPLETE");
            Context.Events.Add(new ActivityEvent(EventType.StartExecution));
        }
    }
}
