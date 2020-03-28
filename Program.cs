using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.DataMovement;
using Microsoft.WindowsAzure.MediaServices.Client;
using System;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;

namespace MultiDatacenterEncode
{
    class Program
    {
        private static CloudMediaContext _mainAccountContext = null;
        private static CloudMediaContext _backupAccountContext = null;
        private static CloudStorageAccount mainStorageAccount = null;
        private static CloudStorageAccount backupStorageAccount = null;
        static void Main(string[] args)
        {
            const int queueThreshold = 3;
            const bool deleteJobsAndFilesFromBackupAccount = true;
            
            // Encoding Profile: Use a standard profile from https://docs.microsoft.com/en-us/azure/media-services/previous/media-services-mes-presets-overview
            // or a custom JSON string for encodeProfile.
            string encodeProfile = "Content Adaptive Multiple Bitrate MP4";

            // This application takes one argument - the local path to the video file you want to encode.
            if (args.Length != 1)
            {
                DisplayUsage();
                return;
            }

            string uploadFile = string.Empty;
            try
            {
                uploadFile = Path.GetFullPath(args[0]);
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: {0}", e.Message);
            }
            
            // Read the App.config settings for the two Media Services accounts and their backing Storage accounts.
            // Then authenticate to all.
            AzureAdTokenCredentials mainTokenCredentials = new AzureAdTokenCredentials(ConfigurationManager.AppSettings["mainAMSAADTenantDomain"],
                new AzureAdClientSymmetricKey(ConfigurationManager.AppSettings["mainAMSClientId"], ConfigurationManager.AppSettings["mainAMSClientSecret"]),
                AzureEnvironments.AzureCloudEnvironment);
            AzureAdTokenCredentials backupTokenCredentials = new AzureAdTokenCredentials(ConfigurationManager.AppSettings["backupAMSAADTenantDomain"],
                new AzureAdClientSymmetricKey(ConfigurationManager.AppSettings["backupAMSClientId"], ConfigurationManager.AppSettings["backupAMSClientSecret"]),
                AzureEnvironments.AzureCloudEnvironment);

            AzureAdTokenProvider mainTokenProvider = new AzureAdTokenProvider(mainTokenCredentials);
            AzureAdTokenProvider backupTokenProvider = new AzureAdTokenProvider(backupTokenCredentials);

            // Specify your REST API endpoint, for example "https://accountname.restv2.westcentralus.media.azure.net/API".      
            _mainAccountContext = new CloudMediaContext(new Uri(ConfigurationManager.AppSettings["mainAMSRESTAPIEndpoint"]), mainTokenProvider);
            _backupAccountContext = new CloudMediaContext(new Uri(ConfigurationManager.AppSettings["backupAMSRESTAPIEndpoint"]), backupTokenProvider);

            mainStorageAccount = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["mainStorageConnectionString"]);
            backupStorageAccount = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["backupStorageConnectionString"]);

            // We always upload to the primary account.  Streaming will be done from here as well.
            IAsset asset = CreateAssetAndUploadSingleFile(AssetCreationOptions.None, uploadFile);

            // Get the encoding stats from the primary Media Services account.
            if (GetEncodingQueue() >= queueThreshold)
            {
                // Use the main/primary datacenter for encoding.
                Console.WriteLine($"starting encoding of {uploadFile}, main account");
                CreateEncodingJob(asset, encodeProfile, _mainAccountContext);
            }
            else
            {

                // Copy the input asset to the backup datacenter, encode it, and copy the results back
                // to the main datacenter.
                Console.WriteLine($"copying {uploadFile} to the backup account");
                IAsset remoteAsset = CopyAsset(asset, CopyDirection.PrimaryToBackup);

                Console.WriteLine($"starting encoding of {uploadFile}, backup account");
                IJob job = CreateEncodingJob(remoteAsset, encodeProfile, _backupAccountContext);

                // The JobStateChanged function will block here until encoding is finished.

                // Copy the encoded asset back to the main Media Services account.
                Console.WriteLine("copying encoded video files to the backup account");
                CopyAsset(job.OutputMediaAssets.FirstOrDefault(), CopyDirection.BackupToPrimary);

                if(deleteJobsAndFilesFromBackupAccount)
                    CleanupBackupAccount(remoteAsset, job);
            }
        }

        // Optionally delete the files from the backup account after the encode job completes.
        private static void CleanupBackupAccount(IAsset asset, IJob job)
        {
            asset.DeleteAsync();
            job.OutputMediaAssets.FirstOrDefault().DeleteAsync();
            job.DeleteAsync();
        }

        // Copy an entire asset / Storage container from one account to the other.
        private static IAsset CopyAsset(IAsset sourceAsset, CopyDirection copyDirection)
        {
            IAsset targetAsset;
            CloudBlobClient targetClient;
            CloudBlobClient sourceClient;

            string SourceContainer = sourceAsset.Uri.AbsolutePath.TrimStart('/');
            if (copyDirection == CopyDirection.PrimaryToBackup)
            {
                targetClient = backupStorageAccount.CreateCloudBlobClient();
                sourceClient = mainStorageAccount.CreateCloudBlobClient();
                targetAsset = _backupAccountContext.Assets.Create(sourceAsset.Name, sourceAsset.Options);
            }
            else
            {
                targetClient = mainStorageAccount.CreateCloudBlobClient();
                sourceClient = backupStorageAccount.CreateCloudBlobClient();
                targetAsset = _mainAccountContext.Assets.Create(sourceAsset.Name, sourceAsset.Options);
            }
            
            string DestinationContainer = targetAsset.Uri.AbsolutePath.TrimStart('/');

            CloudBlobContainer targetContainer = targetClient.GetContainerReference(DestinationContainer);            
            CloudBlobContainer sourceContainer = sourceClient.GetContainerReference(SourceContainer);

            CloudBlobDirectory sourceDirectory = sourceContainer.GetDirectoryReference(string.Empty); //Get root dir
            CloudBlobDirectory targetDirectory = targetContainer.GetDirectoryReference(string.Empty); //Get root dir

            string primaryAssetFile = GetPrimaryFile(sourceAsset);
            CopyContainer(sourceDirectory, targetDirectory);
            UpdateContainer(targetContainer, targetAsset, primaryAssetFile); // This is needed to tell Media Services to update the IAssetFiles list

            return targetAsset;
        }
        private static void CopyContainer(CloudBlobDirectory sourceDirectory, CloudBlobDirectory destDirectory)
        {
            // This function copies an entire Storage container using the data movement library.
            // See https://docs.microsoft.com/en-us/dotnet/api/microsoft.azure.storage.datamovement.copydirectoryoptions?view=azure-dotnet for the API
            // and https://docs.microsoft.com/en-us/azure/storage/common/storage-use-data-movement-library for a general overview.

            ProgressRecorder recorder = new ProgressRecorder();
            Stopwatch stopWatch = Stopwatch.StartNew();

            CopyDirectoryOptions copyDirectoryOptions = new CopyDirectoryOptions
            {
                // Needs to be true to copy the contents of the directory / container.  If this is false the contents will
                // not be copied.
                Recursive = true
            }; 
            // Copy all files under root folder
            DirectoryTransferContext context = new DirectoryTransferContext
            {
                ProgressHandler = recorder
            };
            
            TransferStatus copyStatus = TransferManager.CopyDirectoryAsync(sourceDirectory, destDirectory, CopyMethod.ServiceSideAsyncCopy, copyDirectoryOptions, context).Result;
            stopWatch.Stop();
            // Get the elapsed time as a TimeSpan value.
            TimeSpan ts = stopWatch.Elapsed;

            // Format and display the TimeSpan value.
            string elapsedTime = string.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                ts.Hours, ts.Minutes, ts.Seconds,
                ts.Milliseconds / 10);

            Console.WriteLine($"Elapsed:{elapsedTime} T File transferred: {copyStatus.NumberOfFilesTransferred} total bytes：{copyStatus.BytesTransferred}, failed: {copyStatus.NumberOfFilesFailed}, skipped:{copyStatus.NumberOfFilesSkipped}");
        }

        // Update the Media Services asset with the current contents of the Storage container.
        private static void UpdateContainer(CloudBlobContainer targetContainer, IAsset assetDestination, string primaryFileName)
        {
            foreach (IListBlobItem blob in targetContainer.ListBlobs(blobListingDetails: BlobListingDetails.Metadata))
            {
                CloudBlockBlob targetCloudBlob = (CloudBlockBlob)blob;
                string fileName = targetCloudBlob.Name;
                targetCloudBlob.FetchAttributes();
                if (targetCloudBlob.Properties.Length > 0)
                {
                    IAssetFile assetFile = assetDestination.AssetFiles.Create(fileName);
                    assetFile.ContentFileSize = targetCloudBlob.Properties.Length;
                    if (fileName.ToLower().EndsWith(".ism"))
                    {
                        assetFile.IsPrimary = true;
                    }
                    else if (fileName.ToLower() == primaryFileName.ToLower())
                    {
                        assetFile.IsPrimary = true;
                    }
                    assetFile.Update(); // If using the async version of this method make sure you add an await.
                }
            }
        }

        // Upload the source video and create a new asset.
        static public IAsset CreateAssetAndUploadSingleFile(AssetCreationOptions assetCreationOptions, string singleFilePath)
        {
            // From https://docs.microsoft.com/en-us/azure/media-services/previous/media-services-dotnet-upload-files
            if (!File.Exists(singleFilePath))
            {
                Console.WriteLine("File does not exist.");
                return null;
            }

            var assetName = Path.GetFileNameWithoutExtension(singleFilePath);
            IAsset inputAsset = _mainAccountContext.Assets.Create(assetName, assetCreationOptions);

            var assetFile = inputAsset.AssetFiles.Create(Path.GetFileName(singleFilePath));

            Console.WriteLine("Upload {0}", assetFile.Name);

            assetFile.Upload(singleFilePath);
            Console.WriteLine("Done uploading {0}", assetFile.Name);

            return inputAsset;
        }
        private static string GetPrimaryFile(IAsset assetSource)
        {
            string primary = string.Empty;
            foreach (IAssetFile file in assetSource.AssetFiles)
            {
                if (file.IsPrimary)
                    primary = file.Name;
            }
            return primary;
        }
        private static IJob CreateEncodingJob(IAsset asset, string encodeProfile, CloudMediaContext currentContext)
        {
            string name = asset.Name;

            // Create the job.
            IJob job = currentContext.Jobs.Create("Encoding " + name);

            // Get the current GUID for the Media Encoder Standard.
            IMediaProcessor encoder = GetLatestMediaProcessorByName("Media Encoder Standard");

            ITask task = job.Tasks.AddNew("Encoder Task", encoder, encodeProfile, TaskOptions.None);
            task.InputAssets.Add(asset);
            task.OutputAssets.AddNew(name + " " + encodeProfile, AssetCreationOptions.None);

            // Create an event to listen for job status changes and report those changes back to the console.
            job.StateChanged += new EventHandler<JobStateChangedEventArgs>(JobStateChanged);
            // Submit the encoding job.
            job.Submit();
            // Wait for the job to be completed.
            job.GetExecutionProgressTask(CancellationToken.None).Wait();
            return job;
        }

        // Monitor the job.
        private static void JobStateChanged(object sender, JobStateChangedEventArgs e)
        {
            Console.WriteLine("Job state changed event:");
            Console.WriteLine("  Previous state: " + e.PreviousState);
            Console.WriteLine("  Current state: " + e.CurrentState);
            switch (e.CurrentState)
            {
                case JobState.Finished:
                    Console.WriteLine();
                    Console.WriteLine("Job is finished. Please wait while local tasks or downloads complete...");
                    break;
                case JobState.Canceling:
                case JobState.Queued:
                case JobState.Scheduled:
                case JobState.Processing:
                    Console.WriteLine("Please wait...\n");
                    break;
                case JobState.Canceled:
                case JobState.Error:

                    // Cast sender as a job.
                    IJob job = (IJob)sender;

                    // Display or log error details as needed.
                    break;
                default:
                    break;
            }
        }

        // Gets the number of jobs that are currently in the 'queued' state.
        private static int GetEncodingQueue()
        {
            IQueryable<IJob> queuedJobs = from qjobs in _mainAccountContext.Jobs
                                          where qjobs.State == JobState.Queued
                                          select qjobs;
            return queuedJobs.Count();
        }

        private static IMediaProcessor GetLatestMediaProcessorByName(string mediaProcessorName)
        {
            // It doesn't really matter which account this query comes from since the IMediaProcessor object will be the same for both.
            IMediaProcessor processor = _mainAccountContext.MediaProcessors.Where(p => p.Name == mediaProcessorName).
            ToList().OrderBy(p => new Version(p.Version)).LastOrDefault();

            if (processor == null)
                throw new ArgumentException(string.Format("Unknown media processor", mediaProcessorName));

            return processor;
        }
        private static void DisplayUsage()
        {
            Console.WriteLine("This application requires two Media Services accounts in different datacenters.");
            Console.WriteLine("\nThis application checks the current queue length for videos waiting to be encoded.  If the queue length");
            Console.WriteLine("is greater than the threshold set in the application (default of 3) then a secondary backup datacenter will");
            Console.WriteLine("be used for the encode.  In this case the video is still uploaded to the primary datacenter, the queue");
            Console.WriteLine("length is checked, the video is copied to the backup datacenter, the video is encoded, the resulting video");
            Console.WriteLine("files will be copied back to the primary datacenter and placed into a new asset.");
            Console.WriteLine("\nYou will need to update the App.config in this application with the account information in your two Media");
            Console.WriteLine("Services accounts.");
            Console.WriteLine("\nUsage:");
            Console.WriteLine("MultiDatacenterEncode.exe <LocalPathToVideoToUpload>");
            Console.WriteLine("\n\nExample:\nMultiDatacenterEncode.exe c:\\videos\\myvideo.mp4");
        }

        /// <summary>
        /// A helper class to record progress reported by data movement library.
        /// </summary>
        internal class ProgressRecorder : IProgress<TransferStatus>
        {
            private long latestBytesTransferred;
            private long latestNumberOfFilesTransferred;
            private long latestNumberOfFilesSkipped;
            private long latestNumberOfFilesFailed;

            public long NumberOfFilesProcessed { get; set; }

            public void Report(TransferStatus progress)
            {
                latestBytesTransferred = progress.BytesTransferred;
                latestNumberOfFilesTransferred = progress.NumberOfFilesTransferred;
                latestNumberOfFilesSkipped = progress.NumberOfFilesSkipped;
                latestNumberOfFilesFailed = progress.NumberOfFilesFailed;

                NumberOfFilesProcessed = progress.NumberOfFilesTransferred + progress.NumberOfFilesSkipped + progress.NumberOfFilesFailed;
            }

            public override string ToString()
            {
                return string.Format("Transferred bytes: {0}; Transfered: {1}; Skipped: {2}, Failed: {3}",
                    latestBytesTransferred,
                    latestNumberOfFilesTransferred,
                    latestNumberOfFilesSkipped,
                    latestNumberOfFilesFailed);
            }
        }
        private enum CopyDirection { PrimaryToBackup, BackupToPrimary };
    }
}
