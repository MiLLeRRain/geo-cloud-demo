using System.Collections.Concurrent;
using System.Diagnostics;
using Azure;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.ResourceManager;
using Azure.ResourceManager.ContainerInstance;
using Azure.ResourceManager.ContainerInstance.Models;
using Azure.ResourceManager.Models;
using Azure.ResourceManager.ManagedServiceIdentities;
using Azure.Core;

var builder = WebApplication.CreateBuilder(args);

// Add services
builder.Services.AddSingleton<JobService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<JobService>());

// Register Azure Clients
builder.Services.AddSingleton(sp => {
    var config = sp.GetRequiredService<IConfiguration>();
    var blobEndpoint = config["Azure:Storage:BlobEndpoint"];
    if (!string.IsNullOrEmpty(blobEndpoint))
    {
        return new BlobServiceClient(new Uri(blobEndpoint), new DefaultAzureCredential());
    }
    return null!; // Allow null if not configured
});

builder.Services.AddSingleton(sp => {
    var config = sp.GetRequiredService<IConfiguration>();
    var subId = config["Azure:SubscriptionId"];
    if (!string.IsNullOrEmpty(subId))
    {
        return new ArmClient(new DefaultAzureCredential(), subId);
    }
    return null!;
});

var app = builder.Build();

app.MapPost("/upload", async (IFormFile file, JobService jobService, BlobServiceClient? blobServiceClient, IConfiguration config) =>
{
    if (file == null || file.Length == 0)
        return Results.BadRequest("No file uploaded.");

    var jobId = Guid.NewGuid().ToString();
    string inputLocation;

    // Check if we should use Azure Storage
    var useAzure = !string.IsNullOrEmpty(config["Azure:Storage:BlobEndpoint"]);

    if (useAzure && blobServiceClient != null)
    {
        var containerName = config["Azure:Storage:ContainerName"] ?? "job-data";
        var containerClient = blobServiceClient.GetBlobContainerClient(containerName);
        await containerClient.CreateIfNotExistsAsync();
        
        var blobName = $"{jobId}/{file.FileName}";
        var blobClient = containerClient.GetBlobClient(blobName);
        
        using (var stream = file.OpenReadStream())
        {
            await blobClient.UploadAsync(stream, true);
        }
        inputLocation = blobName; // Store the blob name (relative path)
    }
    else
    {
        // Local Fallback
        var uploadPath = Path.Combine(Directory.GetCurrentDirectory(), "uploads");
        Directory.CreateDirectory(uploadPath);
        var filePath = Path.Combine(uploadPath, $"{jobId}_{file.FileName}");
        using (var stream = new FileStream(filePath, FileMode.Create))
        {
            await file.CopyToAsync(stream);
        }
        inputLocation = filePath;
    }

    // Queue the job
    jobService.QueueJob(jobId, inputLocation);

    return Results.Ok(new { JobId = jobId, Status = "Queued" });
}).DisableAntiforgery();

app.MapGet("/result/{jobId}", async (string jobId, JobService jobService, BlobServiceClient? blobServiceClient, IConfiguration config) =>
{
    var status = jobService.GetJobStatus(jobId);
    if (status == null)
        return Results.NotFound("Job not found.");

    // Clone status to avoid modifying the cached version
    var response = new JobStatus 
    {
        JobId = status.JobId,
        Status = status.Status,
        ResultPath = status.ResultPath,
        SilhouetteScore = status.SilhouetteScore,
        Error = status.Error,
        CreatedAt = status.CreatedAt
    };

    // Security: If Azure is used, generate a temporary SAS token for download
    if (status.Status == "Completed" && !string.IsNullOrEmpty(status.ResultPath) && blobServiceClient != null)
    {
        try 
        {
            var blobUri = new Uri(status.ResultPath);
            // Verify this is our storage account
            if (blobUri.Host == blobServiceClient.Uri.Host)
            {
                // Parse container and blob name from URI
                // Format: https://account.blob.core.windows.net/container/path/to/blob
                var pathSegments = blobUri.AbsolutePath.TrimStart('/').Split('/', 2);
                if (pathSegments.Length == 2)
                {
                    var containerName = pathSegments[0];
                    var blobName = pathSegments[1];

                    // Generate short-lived SAS (15 mins)
                    var userDelegationKey = await blobServiceClient.GetUserDelegationKeyAsync(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddMinutes(15));
                    var sasBuilder = new Azure.Storage.Sas.BlobSasBuilder
                    {
                        BlobContainerName = containerName,
                        BlobName = blobName,
                        Resource = "b",
                        StartsOn = DateTimeOffset.UtcNow,
                        ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(15)
                    };
                    sasBuilder.SetPermissions(Azure.Storage.Sas.BlobSasPermissions.Read);
                    
                    var sasQuery = sasBuilder.ToSasQueryParameters(userDelegationKey, blobServiceClient.AccountName).ToString();
                    response.ResultPath = $"{status.ResultPath}?{sasQuery}";
                }
            }
        }
        catch (Exception ex)
        {
            // Fallback: return original URL (client needs their own auth)
            Console.WriteLine($"SAS Generation failed: {ex.Message}");
        }
    }

    return Results.Ok(response);
});

// New Endpoint: List active Azure Jobs (Recover state if API restarts)
app.MapGet("/azure-jobs", async (ArmClient? armClient, IConfiguration config) =>
{
    if (armClient == null) return Results.Ok(new List<object>());
    
    var rgName = config["Azure:ResourceGroup"];
    var subscription = await armClient.GetDefaultSubscriptionAsync();
    var resourceGroup = await subscription.GetResourceGroupAsync(rgName);
    
    var activeJobs = new List<object>();
    // List all container groups starting with "job-"
    await foreach (var group in resourceGroup.Value.GetContainerGroups().GetAllAsync())
    {
        if (group.Data.Name.StartsWith("job-"))
        {
            activeJobs.Add(new { 
                JobId = group.Data.Name.Replace("job-", ""),
                Name = group.Data.Name,
                State = group.Data.InstanceView.State,
                ProvisioningState = group.Data.ProvisioningState,
                Created = group.Data.InstanceView.Events.FirstOrDefault()?.LastTimestamp
            });
        }
    }
    return Results.Ok(activeJobs);
});

// New Endpoint: Get Logs directly from Azure (Works even if API restarted)
app.MapGet("/azure-logs/{jobId}", async (string jobId, ArmClient? armClient, IConfiguration config) =>
{
    if (armClient == null) return Results.NotFound("Azure not configured");
    
    var rgName = config["Azure:ResourceGroup"];
    var subscription = await armClient.GetDefaultSubscriptionAsync();
    var resourceGroup = await subscription.GetResourceGroupAsync(rgName);
    var containerGroupName = $"job-{jobId}";
    
    try 
    {
        var containerGroup = await resourceGroup.Value.GetContainerGroups().GetAsync(containerGroupName);
        // Fetch logs for the 'geo-worker' container
        var logs = await containerGroup.Value.GetContainerLogsAsync("geo-worker", 4000); 
        return Results.Text(logs.Value.Content);
    }
    catch (Exception ex)
    {
        return Results.Problem($"Failed to get logs: {ex.Message}");
    }
});

app.Run();

public class JobStatus
{
    public string JobId { get; set; } = string.Empty;
    public string Status { get; set; } = "Queued"; // Queued, Running, Completed, Failed
    public string? ResultPath { get; set; }
    public double? SilhouetteScore { get; set; }
    public string? Error { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

public class Metrics
{
    [System.Text.Json.Serialization.JsonPropertyName("silhouette_score")]
    public double SilhouetteScore { get; set; }
}

public class JobService : BackgroundService
{
    private readonly ConcurrentDictionary<string, JobStatus> _jobs = new();
    private readonly System.Threading.Channels.Channel<(string JobId, string InputLocation)> _queue;
    private readonly ILogger<JobService> _logger;
    private readonly IConfiguration _configuration;
    private readonly BlobServiceClient? _blobServiceClient;
    private readonly ArmClient? _armClient;
    private readonly string _pythonPath; 

    public JobService(ILogger<JobService> logger, IConfiguration configuration, BlobServiceClient? blobServiceClient = null, ArmClient? armClient = null)
    {
        _logger = logger;
        _configuration = configuration;
        _blobServiceClient = blobServiceClient;
        _armClient = armClient;
        _queue = System.Threading.Channels.Channel.CreateUnbounded<(string, string)>();
        _pythonPath = "python"; 
    }

    public void QueueJob(string jobId, string inputLocation)
    {
        _jobs[jobId] = new JobStatus { JobId = jobId, Status = "Queued" };
        _queue.Writer.TryWrite((jobId, inputLocation));
    }

    public JobStatus? GetJobStatus(string jobId)
    {
        _jobs.TryGetValue(jobId, out var status);
        return status;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var (jobId, inputLocation) in _queue.Reader.ReadAllAsync(stoppingToken))
        {
            try
            {
                UpdateStatus(jobId, "Running");
                _logger.LogInformation($"Starting job {jobId} processing {inputLocation}");

                bool useAzure = _blobServiceClient != null && _armClient != null && !string.IsNullOrEmpty(_configuration["Azure:SubscriptionId"]);
                
                if (useAzure)
                {
                    await RunAzureJob(jobId, inputLocation);
                }
                else
                {
                    await RunLocalJob(jobId, inputLocation);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Job {jobId} failed.");
                UpdateStatus(jobId, "Failed", error: ex.Message);
            }
        }
    }

    private async Task RunAzureJob(string jobId, string blobName)
    {
        var containerName = _configuration["Azure:Storage:ContainerName"] ?? "job-data";
        var containerClient = _blobServiceClient!.GetBlobContainerClient(containerName);
        var inputBlob = containerClient.GetBlobClient(blobName);
        
        // Get User Delegation Key for SAS
        var userDelegationKey = await _blobServiceClient.GetUserDelegationKeyAsync(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddHours(2));

        // Generate SAS for Input (Read)
        var sasBuilderInput = new Azure.Storage.Sas.BlobSasBuilder
        {
            BlobContainerName = containerName,
            BlobName = inputBlob.Name,
            Resource = "b",
            StartsOn = DateTimeOffset.UtcNow,
            ExpiresOn = DateTimeOffset.UtcNow.AddHours(2)
        };
        sasBuilderInput.SetPermissions(Azure.Storage.Sas.BlobSasPermissions.Read);
        var inputSasUri = new UriBuilder(inputBlob.Uri)
        {
            Query = sasBuilderInput.ToSasQueryParameters(userDelegationKey, _blobServiceClient.AccountName).ToString()
        }.Uri;
        
        var outputBlobName = $"{jobId}/results.csv";
        var metricsBlobName = $"{jobId}/metrics.json";
        var outputBlob = containerClient.GetBlobClient(outputBlobName);
        var metricsBlob = containerClient.GetBlobClient(metricsBlobName);
        
        // Generate SAS for Output (Write)
        var sasBuilderOutput = new Azure.Storage.Sas.BlobSasBuilder
        {
            BlobContainerName = containerName,
            BlobName = outputBlobName,
            Resource = "b",
            StartsOn = DateTimeOffset.UtcNow,
            ExpiresOn = DateTimeOffset.UtcNow.AddHours(2)
        };
        sasBuilderOutput.SetPermissions(Azure.Storage.Sas.BlobSasPermissions.Write | Azure.Storage.Sas.BlobSasPermissions.Create);
        var outputSasUri = new UriBuilder(outputBlob.Uri)
        {
            Query = sasBuilderOutput.ToSasQueryParameters(userDelegationKey, _blobServiceClient.AccountName).ToString()
        }.Uri;

        // Generate SAS for Metrics (Write)
        var sasBuilderMetrics = new Azure.Storage.Sas.BlobSasBuilder
        {
            BlobContainerName = containerName,
            BlobName = metricsBlobName,
            Resource = "b",
            StartsOn = DateTimeOffset.UtcNow,
            ExpiresOn = DateTimeOffset.UtcNow.AddHours(2)
        };
        sasBuilderMetrics.SetPermissions(Azure.Storage.Sas.BlobSasPermissions.Write | Azure.Storage.Sas.BlobSasPermissions.Create);
        var metricsSasUri = new UriBuilder(metricsBlob.Uri)
        {
            Query = sasBuilderMetrics.ToSasQueryParameters(userDelegationKey, _blobServiceClient.AccountName).ToString()
        }.Uri;

        var rgName = _configuration["Azure:ResourceGroup"];
        var acrLoginServer = _configuration["Azure:ContainerRegistry:LoginServer"];
        var acrImageName = _configuration["Azure:ContainerRegistry:ImageName"];
        var fullImage = $"{acrLoginServer}/{acrImageName}";
        
        var subscription = await _armClient!.GetDefaultSubscriptionAsync();
        var resourceGroup = await subscription.GetResourceGroupAsync(rgName);
        
        var containerGroupName = $"job-{jobId}";
        
        var resources = new ContainerResourceRequirements(new ContainerResourceRequestsContent(1.5, 1.0));
        var containerInstance = new ContainerInstanceContainer("geo-worker", fullImage, resources)
        {
            EnvironmentVariables = 
            {
                new ContainerEnvironmentVariable("INPUT_SAS_URL") { Value = inputSasUri.ToString() },
                new ContainerEnvironmentVariable("OUTPUT_SAS_URL") { Value = outputSasUri.ToString() },
                new ContainerEnvironmentVariable("METRICS_SAS_URL") { Value = metricsSasUri.ToString() }
            }
        };

        var containerGroupData = new ContainerGroupData(Azure.Core.AzureLocation.AustraliaEast, new[] { containerInstance }, ContainerInstanceOperatingSystemType.Linux)
        {
            RestartPolicy = ContainerGroupRestartPolicy.Never
        };
        
        // Add Managed Identity for ACR Pull
        var identityName = _configuration["Azure:ManagedIdentityName"] ?? "id-geospark-worker";
        var identity = await resourceGroup.Value.GetUserAssignedIdentities().GetAsync(identityName);
        var identityId = identity.Value.Id;

        containerGroupData.Identity = new ManagedServiceIdentity(ManagedServiceIdentityType.UserAssigned)
        {
            UserAssignedIdentities = { { identityId, new UserAssignedIdentity() } }
        };
        containerGroupData.ImageRegistryCredentials.Add(new ContainerGroupImageRegistryCredential(acrLoginServer)
        {
            Identity = identityId.ToString()
        });

        var containerGroups = resourceGroup.Value.GetContainerGroups();
        var operation = await containerGroups.CreateOrUpdateAsync(WaitUntil.Completed, containerGroupName, containerGroupData);
        var containerGroupResource = operation.Value;

        // Poll for completion
        while (true)
        {
            await Task.Delay(5000);
            containerGroupResource = await containerGroupResource.GetAsync();
            var state = containerGroupResource.Data.InstanceView.State;
            
            if (state == "Succeeded")
            {
                double? score = null;
                try {
                    if (await metricsBlob.ExistsAsync())
                    {
                        var download = await metricsBlob.DownloadContentAsync();
                        var json = download.Value.Content.ToString();
                        var metrics = System.Text.Json.JsonSerializer.Deserialize<Metrics>(json);
                        score = metrics?.SilhouetteScore;
                    }
                } catch {}

                UpdateStatus(jobId, "Completed", resultPath: outputBlob.Uri.ToString(), score: score);
                break;
            }
            else if (state == "Failed")
            {
                UpdateStatus(jobId, "Failed", error: "Container failed");
                break;
            }
        }
        
        // Cleanup
        // await containerGroupResource.DeleteAsync(WaitUntil.Completed);
        _logger.LogInformation($"Container group {containerGroupName} finished. Retaining for debugging.");
    }

    private async Task RunLocalJob(string jobId, string filePath)
    {
        var outputDir = Path.Combine(Directory.GetCurrentDirectory(), "outputs", jobId);
        Directory.CreateDirectory(outputDir);

        _logger.LogInformation($"Starting local job {jobId} processing {filePath}");

        bool useDocker = _configuration.GetValue<bool>("UseDocker");
        ProcessStartInfo startInfo;

        if (useDocker)
        {
            var uploadsDir = Path.GetDirectoryName(filePath);
            var fileName = Path.GetFileName(filePath);
            var outputsBaseDir = Path.Combine(Directory.GetCurrentDirectory(), "outputs");

            var dockerArgs = $"run --rm " +
                             $"-v \"{uploadsDir}:/data/inputs\" " +
                             $"-v \"{outputsBaseDir}:/data/outputs\" " +
                             $"geo-spark-ml:latest " +
                             $"--input \"/data/inputs/{fileName}\" " +
                             $"--output \"/data/outputs/{jobId}\"";

            startInfo = new ProcessStartInfo
            {
                FileName = "docker",
                Arguments = dockerArgs,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };
        }
        else
        {
            var srcPath = Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), "..", "geo-spark-ml", "src"));
            startInfo = new ProcessStartInfo
            {
                FileName = _pythonPath,
                Arguments = $"-m geo_clustering.main --input \"{filePath}\" --output \"{outputDir}\"",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
                WorkingDirectory = srcPath
            };
            startInfo.EnvironmentVariables["PYTHONPATH"] = srcPath;
        }

        using var process = new Process { StartInfo = startInfo };
        
        process.Start();
        string output = await process.StandardOutput.ReadToEndAsync();
        string error = await process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();

        if (process.ExitCode == 0)
        {
            double? score = null;
            var metricsPath = Path.Combine(outputDir, "metrics.json");
            if (File.Exists(metricsPath))
            {
                try 
                {
                    var json = await File.ReadAllTextAsync(metricsPath);
                    var metrics = System.Text.Json.JsonSerializer.Deserialize<Metrics>(json);
                    score = metrics?.SilhouetteScore;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to parse metrics.json");
                }
            }

            UpdateStatus(jobId, "Completed", resultPath: outputDir, score: score);
            _logger.LogInformation($"Job {jobId} completed successfully.");
        }
        else
        {
            UpdateStatus(jobId, "Failed", error: error + "\n" + output);
            _logger.LogError($"Job {jobId} failed: {error}\nOutput: {output}");
        }
    }

    private void UpdateStatus(string jobId, string status, string? resultPath = null, string? error = null, double? score = null)
    {
        if (_jobs.TryGetValue(jobId, out var job))
        {
            job.Status = status;
            if (resultPath != null) job.ResultPath = resultPath;
            if (error != null) job.Error = error;
            if (score != null) job.SilhouetteScore = score;
        }
    }
}
