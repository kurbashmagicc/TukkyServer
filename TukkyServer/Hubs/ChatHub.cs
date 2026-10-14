using Microsoft.AspNetCore.SignalR;
using System.Collections.Concurrent;
using System.Text.Json;

namespace TukkyServer.Hubs;

public class UserProfileDto
{
    public string UserId { get; set; } = string.Empty;
    public string Tag { get; set; } = string.Empty;
    public string RealName { get; set; } = string.Empty;
    public string Bio { get; set; } = string.Empty;
    public string Phone { get; set; } = string.Empty;
    public string AvatarBase64 { get; set; } = string.Empty;
    public bool IsPrivate { get; set; }
}

public class RegisterResultDto
{
    public bool Success { get; set; }
    public string Message { get; set; } = string.Empty;
    public UserProfileDto? Profile { get; set; }
}

public class PendingMessageDto
{
    public string SenderTag { get; set; } = string.Empty;
    public string Text { get; set; } = string.Empty;
}

public class ChatHub : Hub
{
    private const string DbFilePath = "users_db.json";
    private static readonly ConcurrentDictionary<string, UserProfileDto> UsersDb = LoadDatabase();
    private static readonly ConcurrentDictionary<string, string> OnlineUsers = new();
    private static readonly ConcurrentDictionary<string, List<PendingMessageDto>> OfflineMessages = new();

    private static ConcurrentDictionary<string, UserProfileDto> LoadDatabase()
    {
        try
        {
            if (File.Exists(DbFilePath))
            {
                string json = File.ReadAllText(DbFilePath);
                var dict = JsonSerializer.Deserialize<Dictionary<string, UserProfileDto>>(json);
                if (dict != null)
                    return new ConcurrentDictionary<string, UserProfileDto>(dict, StringComparer.OrdinalIgnoreCase);
            }
        }
        catch { }
        return new ConcurrentDictionary<string, UserProfileDto>(StringComparer.OrdinalIgnoreCase);
    }

    private static void SaveDatabase()
    {
        try
        {
            string json = JsonSerializer.Serialize(UsersDb);
            File.WriteAllText(DbFilePath, json);
        }
        catch { }
    }

    private static string NormalizeTag(string tag)
    {
        return (tag ?? string.Empty).Trim().TrimStart('@').ToLowerInvariant();
    }

    public async Task<RegisterResultDto> RegisterOrUpdateUser(UserProfileDto profile)
    {
        string cleanTag = NormalizeTag(profile.Tag);
        if (string.IsNullOrEmpty(cleanTag))
        {
            return new RegisterResultDto { Success = false, Message = "Ник не может быть пустым." };
        }

        if (UsersDb.TryGetValue(cleanTag, out var existingUser) &&
            !string.IsNullOrEmpty(existingUser.UserId) &&
            existingUser.UserId != profile.UserId)
        {
            Console.WriteLine($"[КОНСОЛЬ СЕРВЕРА] Отказ: ник @{cleanTag} уже занят!");
            return new RegisterResultDto
            {
                Success = false,
                Message = $"Ник @{cleanTag} уже занят другим пользователем!"
            };
        }

        var oldEntries = UsersDb.Where(x => x.Value.UserId == profile.UserId && x.Key != cleanTag).ToList();
        foreach (var old in oldEntries)
        {
            UsersDb.TryRemove(old.Key, out _);
            OnlineUsers.TryRemove(old.Key, out _);
        }

        profile.Tag = cleanTag;
        UsersDb[cleanTag] = profile;
        OnlineUsers[cleanTag] = Context.ConnectionId;
        SaveDatabase();

        Console.WriteLine($"[КОНСОЛЬ СЕРВЕРА] Пользователь в сети: @{cleanTag} ({profile.RealName})");

        if (OfflineMessages.TryRemove(cleanTag, out var pendingList))
        {
            foreach (var msg in pendingList)
            {
                UsersDb.TryGetValue(msg.SenderTag, out var senderProfile);
                await Clients.Caller.SendAsync("ReceiveMessageWithProfile", msg.SenderTag, msg.Text, senderProfile);
            }
        }

        return new RegisterResultDto { Success = true, Message = "OK", Profile = profile };
    }

    public Task<UserProfileDto?> FindUser(string searchTag)
    {
        string cleanTag = NormalizeTag(searchTag);
        Console.WriteLine($"[КОНСОЛЬ СЕРВЕРА] Поиск ника: @{cleanTag}");

        if (string.IsNullOrEmpty(cleanTag))
            return Task.FromResult<UserProfileDto?>(null);

        if (UsersDb.TryGetValue(cleanTag, out var user) && !user.IsPrivate)
        {
            Console.WriteLine($"[КОНСОЛЬ СЕРВЕРА] Найдено: @{cleanTag}");
            return Task.FromResult<UserProfileDto?>(user);
        }

        Console.WriteLine($"[КОНСОЛЬ СЕРВЕРА] Не найдено: @{cleanTag}");
        return Task.FromResult<UserProfileDto?>(null);
    }

    public async Task SendMessageToUser(string senderUsername, string targetUsername, string messageText)
    {
        string cleanSender = NormalizeTag(senderUsername);
        string cleanTarget = NormalizeTag(targetUsername);

        Console.WriteLine($"[КОНСОЛЬ СЕРВЕРА] Сообщение @{cleanSender} -> @{cleanTarget}: {messageText}");

        UsersDb.TryGetValue(cleanSender, out var senderProfile);

        if (OnlineUsers.TryGetValue(cleanTarget, out string? targetConnectionId))
        {
            await Clients.Client(targetConnectionId).SendAsync("ReceiveMessageWithProfile", cleanSender, messageText, senderProfile);
        }
        else
        {
            var list = OfflineMessages.GetOrAdd(cleanTarget, _ => new List<PendingMessageDto>());
            lock (list)
            {
                list.Add(new PendingMessageDto { SenderTag = cleanSender, Text = messageText });
            }
        }
    }

    public override Task OnDisconnectedAsync(Exception? exception)
    {
        var user = OnlineUsers.FirstOrDefault(x => x.Value == Context.ConnectionId);
        if (user.Key != null)
        {
            OnlineUsers.TryRemove(user.Key, out _);
            Console.WriteLine($"[КОНСОЛЬ СЕРВЕРА] Отключился: @{user.Key}");
        }
        return base.OnDisconnectedAsync(exception);
    }
}
