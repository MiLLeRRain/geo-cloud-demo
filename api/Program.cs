using System.Collections.Concurrent;
using System.Diagnostics;

var builder = WebApplication.CreateBuilder(args);

// Add services
builder.Services.AddSingleton<JobService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<JobService>());

var app = builder.Build();

app.MapPost("/upload", async (IFormFile file, JobService jobService) =>
{
    if (file == null || file.Length == 0)
        return Results.BadRequest("No file uploaded.");

    var jobId = Guid.NewGuid().ToString();
    var uploadPath = Path.Combine(Directory.GetCurrentDirectory(), "uploads");
    Directory.CreateDirectory(uploadPath);
    
    var filePath = Path.Combine(uploadPath, $"{jobId}_{file.FileName}");
    
    using (var stream = new FileStream(filePath, FileMode.Create))
    {
        await file.CopyToAsync(stream);
    }

    // Queue the job
    jobService.QueueJob(jobId, filePath);

    return Results.Ok(new { JobId = jobId, Status = "Queued" });
}).DisableAntiforgery();

app.MapGet("/result/{jobId}", (string jobId, JobService jobService) =>
{
    var status = jobService.GetJobStatus(jobId);
    if (status == null)
        return Results.NotFound("Job not found.");

    return Results.Ok(status);
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
    private readonly System.Threading.Channels.Channel<(string JobId, string FilePath)> _queue;
    private readonly ILogger<JobService> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _pythonPath; 

    public JobService(ILogger<JobService> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _queue = System.Threading.Channels.Channel.CreateUnbounded<(string, string)>();
        
        // Assume python is in PATH. In a real scenario, this might be configured.
        _pythonPath = "python"; 
    }

    public void QueueJob(string jobId, string filePath)
    {
        _jobs[jobId] = new JobStatus { JobId = jobId, Status = "Queued" };
        _queue.Writer.TryWrite((jobId, filePath));
    }

    public JobStatus? GetJobStatus(string jobId)
    {
        _jobs.TryGetValue(jobId, out var status);
        return status;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var (jobId, filePath) in _queue.Reader.ReadAllAsync(stoppingToken))
        {
            try
            {
                UpdateStatus(jobId, "Running");
                
                var outputDir = Path.Combine(Directory.GetCurrentDirectory(), "outputs", jobId);
                Directory.CreateDirectory(outputDir);

                _logger.LogInformation($"Starting job {jobId} processing {filePath}");

                bool useDocker = _configuration.GetValue<bool>("UseDocker");
                ProcessStartInfo startInfo;

                if (useDocker)
                {
                    // Docker Execution
                    var uploadsDir = Path.GetDirectoryName(filePath);
                    var fileName = Path.GetFileName(filePath);
                    var outputsBaseDir = Path.Combine(Directory.GetCurrentDirectory(), "outputs");

                    // Docker command arguments
                    // We mount the uploads folder to /data/inputs
                    // We mount the outputs folder to /data/outputs
                    // Note: On Windows, paths might need adjustment for Docker Desktop, but usually absolute paths work.
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
                    _logger.LogInformation($"Running Docker: docker {dockerArgs}");
                }
                else
                {
                    // Local Python Execution
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
            catch (Exception ex)
            {
                UpdateStatus(jobId, "Failed", error: ex.Message);
                _logger.LogError(ex, $"Error processing job {jobId}");
            }
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
