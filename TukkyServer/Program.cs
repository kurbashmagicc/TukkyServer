using TukkyServer.Hubs;

var builder = WebApplication.CreateBuilder(args);

// Разрешаем подключения с любых мобильных клиентов
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

builder.Services.AddSignalR();

var app = builder.Build();

app.UseCors();

// Маршрут для проверки жизнеспособности сервера (health-check)
app.MapGet("/", () => "Tukky Server is running!");

// Основной хаб веб-сокетов
app.MapHub<ChatHub>("/chathub");

// Читаем порт из переменной окружения Render или ставим 5000 по умолчанию
var port = Environment.GetEnvironmentVariable("PORT") ?? "5000";
app.Run($"http://0.0.0.0:{port}");