using TukkyServer.Hubs;

var builder = WebApplication.CreateBuilder(args);

builder.WebHost.ConfigureKestrel(options =>
{
    options.Limits.MaxRequestBodySize = 100 * 1024 * 1024; // 100 МБ
});

builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        policy.AllowAnyHeader()
              .AllowAnyMethod()
              .AllowCredentials()
              .SetIsOriginAllowed(_ => true);
    });
});

// Лимит пакета SignalR — 100 МБ для тяжелых фото и видео
builder.Services.AddSignalR(options =>
{
    options.MaximumReceiveMessageSize = 100 * 1024 * 1024;
});

var app = builder.Build();

app.UseCors();

app.MapGet("/", () => "Tukky Server is running!");
app.MapHub<ChatHub>("/chathub");

var port = Environment.GetEnvironmentVariable("PORT") ?? "5000";
app.Run($"http://0.0.0.0:{port}");
