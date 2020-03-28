# MultiDatacenterEncode
**Using Azure Media Services this allows you to encode videos using a second datacenter if your jobs are queuing in your primary datacenter.**

This application was written in C#.NET.  It uses the Azure Media Services [v2 API](https://docs.microsoft.com/en-us/azure/media-services/previous/).

To use the application you need to have two Media Services accounts.  One is your primary Media Services account and one is a backup account located in a less utilized datacenter.  You’ll need to update the App.config with both the Media Services account information as well as the Storage account connection string.  

The general workflow is that the application uploads a local video file to the primary Media Services account.  Next it checks the primary account’s job queue.  If there are more than 3 (configurable) jobs queued then the application copies the asset to the backup account.  An encode job is kicked off in the backup account.  Once the job is done the video is copied back from the backup Media Services account to the primary Media Services account.  If the job queue is less than 3 then the primary Media Services account is used for encoding.

This is not meant to be production ready, but rather an outline for how it is possible to use multiple datacenters for encoding.  Also note that copying assets between datacenters will incur additional egress (bandwidth) costs.  Generally these are relatively low, but there is some additional cost in doing this.

The big benefit to this application beyond the ability to use a second datacenter for encoding is that you are still delivering content from the main primary account.  The backup account is really just a place to do some extra work.
