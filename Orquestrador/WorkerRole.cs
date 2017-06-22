using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;

namespace Orquestrador
{
    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);

        static CloudQueue facadeQueue;
        static CloudQueue backEndQueue;

        public override void Run()
        {
            Trace.TraceInformation("Orquestrador is running");


            var connectionString = "DefaultEndpointsProtocol=https;AccountName=aulapucramon;AccountKey=NEpVr5B2s7kWfBy+osHruQLpHfw+vsozTZZH/QiltLMo7HrbGiFDTz1haYhvf/JXXVuyJ6YFVdQIYOHyN+wYHw==;EndpointSuffix=core.windows.net";

            CloudStorageAccount cloudStorageAccount;

            if (!CloudStorageAccount.TryParse(connectionString, out cloudStorageAccount))
            {

            }

            var cloudQueueClient = cloudStorageAccount.CreateCloudQueueClient();

            facadeQueue = cloudQueueClient.GetQueueReference("facadequeue");
            facadeQueue.CreateIfNotExists();

            backEndQueue = cloudQueueClient.GetQueueReference("backendqueue");
            backEndQueue.CreateIfNotExists();

            try
            {
                this.RunAsync(this.cancellationTokenSource.Token).Wait();
            }
            finally
            {
                this.runCompleteEvent.Set();
            }
        }

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections
            ServicePointManager.DefaultConnectionLimit = 12;

            // For information on handling configuration changes
            // see the MSDN topic at https://go.microsoft.com/fwlink/?LinkId=166357.

            bool result = base.OnStart();

            Trace.TraceInformation("Orquestrador has been started");

            return result;
        }

        public override void OnStop()
        {
            Trace.TraceInformation("Orquestrador is stopping");

            this.cancellationTokenSource.Cancel();
            this.runCompleteEvent.WaitOne();

            base.OnStop();

            Trace.TraceInformation("Orquestrador has stopped");
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            // TODO: Replace the following with your own logic.
            while (!cancellationToken.IsCancellationRequested)
            {
                Trace.TraceInformation("Orquestrador Working");

                var connectionString = "DefaultEndpointsProtocol=https;AccountName=aulapucramon;AccountKey=NEpVr5B2s7kWfBy+osHruQLpHfw+vsozTZZH/QiltLMo7HrbGiFDTz1haYhvf/JXXVuyJ6YFVdQIYOHyN+wYHw==;EndpointSuffix=core.windows.net";

                CloudStorageAccount cloudStorageAccount;

                if (!CloudStorageAccount.TryParse(connectionString, out cloudStorageAccount))
                {

                }
                var cloudQueueClient = cloudStorageAccount.CreateCloudQueueClient();
                var facadeQueue = cloudQueueClient.GetQueueReference("facadequeue");
                facadeQueue.CreateIfNotExists();
                
                var backEndQueue = cloudQueueClient.GetQueueReference("backendqueue");
                backEndQueue.CreateIfNotExists();

                var cloudQueueMessage = facadeQueue.GetMessage();

                if (cloudQueueMessage == null)
                {
                    return;
                }
                else
                {
                    facadeQueue.DeleteMessage(cloudQueueMessage);
                    var message = new CloudQueueMessage("new message");
                    backEndQueue.AddMessage(message);
                }                

                // 10 seconds frequency
                await Task.Delay(10000);
            }
        }
    }
}
