using TukkyServer.Hubs;

var builder = WebApplication.CreateBuilder(args);

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

// Увеличиваем лимит пакета до 30 МБ для пересылки фото, голосовых и видео
builder.Services.AddSignalR(options =>
{
    options.MaximumReceiveMessageSize = 30 * 1024 * 1024;
});

var app = builder.Build();

app.UseCors();

app.MapGet("/", () => "Tukky Server is running!");
app.MapHub<ChatHub>("/chathub");

var port = Environment.GetEnvironmentVariable("PORT") ?? "5000";
app.Run($"http://0.0.0.0:{port}");
