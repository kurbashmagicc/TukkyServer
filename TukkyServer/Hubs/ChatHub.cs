using Microsoft.AspNetCore.SignalR;
using System.Collections.Concurrent;
using System.Text.Json;

namespace TukkyServer.Hubs;

public class UserProfileDto
{
    public string UserId { get; set; } = string.Empty;
    public string Tag { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
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

public class NetworkMessageDto
{
    public string MessageId { get; set; } = string.Empty;
    public string SenderTag { get; set; } = string.Empty;
    public string TargetTag { get; set; } = string.Empty;
    public string Text { get; set; } = string.Empty;
    public string MessageType { get; set; } = "text";
    public string MediaBase64 { get; set; } = string.Empty;
    public string FileExtension { get; set; } = string.Empty;
    public double AudioDurationSeconds { get; set; }
}

public class ChatHub : Hub
{
    private const string DbFilePath = "users_db.json";
    private const string PendingFilePath = "pending_messages.json";
    private static readonly object FileLock = new();

    private static readonly ConcurrentDictionary<string, UserProfileDto> UsersDb = LoadDatabase();
    private static readonly ConcurrentDictionary<string, string> OnlineUsers = new(StringComparer.OrdinalIgnoreCase);

    // Очередь гарантированной доставки: сообщения хранятся здесь, пока телефон не пришлет AckMessage
    private static readonly ConcurrentDictionary<string, List<NetworkMessageDto>> PendingMessages = LoadPendingMessages();
    private static readonly ConcurrentDictionary<string, List<(string SenderTag, string MessageId)>> OfflineDeletions = new(StringComparer.OrdinalIgnoreCase);
    private static readonly ConcurrentDictionary<string, HashSet<string>> OfflineReadReceipts = new(StringComparer.OrdinalIgnoreCase);

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

    private static ConcurrentDictionary<string, List<NetworkMessageDto>> LoadPendingMessages()
    {
        try
        {
            if (File.Exists(PendingFilePath))
            {
                string json = File.ReadAllText(PendingFilePath);
                var dict = JsonSerializer.Deserialize<Dictionary<string, List<NetworkMessageDto>>>(json);
                if (dict != null)
                    return new ConcurrentDictionary<string, List<NetworkMessageDto>>(dict, StringComparer.OrdinalIgnoreCase);
            }
        }
        catch { }
        return new ConcurrentDictionary<string, List<NetworkMessageDto>>(StringComparer.OrdinalIgnoreCase);
    }

    private static void SaveDatabase()
    {
        lock (FileLock)
        {
            try
            {
                File.WriteAllText(DbFilePath, JsonSerializer.Serialize(UsersDb));
            }
            catch { }
        }
    }

    private static void SavePendingMessages()
    {
        lock (FileLock)
        {
            try
            {
                File.WriteAllText(PendingFilePath, JsonSerializer.Serialize(PendingMessages));
            }
            catch { }
        }
    }

    private static string NormalizeTag(string tag)
    {
        return (tag ?? string.Empty).Trim().TrimStart('@').ToLowerInvariant();
    }

    // Фоновый пинг + автоматическая досылка всех неподтвержденных сообщений
    public async Task PingUser(string tag)
    {
        string cleanTag = NormalizeTag(tag);
        if (!string.IsNullOrEmpty(cleanTag))
        {
            OnlineUsers[cleanTag] = Context.ConnectionId;
            await DeliverPendingEventsAsync(cleanTag);
        }
    }

    // Подтверждение от телефона, что сообщение реально сохранено в память устройства
    public Task AckMessage(string receiverTag, string messageId)
    {
        string cleanReceiver = NormalizeTag(receiverTag);
        if (string.IsNullOrEmpty(cleanReceiver) || string.IsNullOrEmpty(messageId))
            return Task.CompletedTask;

        if (PendingMessages.TryGetValue(cleanReceiver, out var list))
        {
            bool removed = false;
            lock (list)
            {
                removed = list.RemoveAll(m => m.MessageId == messageId) > 0;
            }
            if (removed)
            {
                SavePendingMessages();
            }
        }
        return Task.CompletedTask;
    }

    public async Task<RegisterResultDto> LoginUser(string tag, string password, string currentDeviceId)
    {
        string cleanTag = NormalizeTag(tag);
        if (string.IsNullOrEmpty(cleanTag))
        {
            return new RegisterResultDto { Success = false, Message = "Введите никнейм." };
        }

        if (!UsersDb.TryGetValue(cleanTag, out var existingUser))
        {
            return new RegisterResultDto { Success = false, Message = $"Аккаунт @{cleanTag} не найден. Сначала зарегистрируйтесь." };
        }

        if (!string.IsNullOrEmpty(existingUser.Password) && existingUser.Password != password)
        {
            return new RegisterResultDto { Success = false, Message = "Неверный пароль от аккаунта!" };
        }

        if (string.IsNullOrEmpty(existingUser.Password) && !string.IsNullOrEmpty(password))
        {
            existingUser.Password = password;
        }

        if (string.IsNullOrEmpty(existingUser.UserId))
        {
            existingUser.UserId = currentDeviceId;
        }

        UsersDb[cleanTag] = existingUser;
        OnlineUsers[cleanTag] = Context.ConnectionId;
        SaveDatabase();

        Console.WriteLine($"[КОНСОЛЬ СЕРВЕРА] Вход в аккаунт: @{cleanTag} ({existingUser.RealName})");

        await DeliverPendingEventsAsync(cleanTag);

        return new RegisterResultDto { Success = true, Message = "OK", Profile = existingUser };
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
            return new RegisterResultDto
            {
                Success = false,
                Message = $"Ник @{cleanTag} уже занят! Если это ваш аккаунт, переключитесь на вкладку «Вход»."
            };
        }

        if (existingUser != null)
        {
            if (string.IsNullOrEmpty(profile.Password))
                profile.Password = existingUser.Password;
            if (string.IsNullOrEmpty(profile.AvatarBase64))
                profile.AvatarBase64 = existingUser.AvatarBase64;
        }

        var oldEntries = UsersDb.Where(x => x.Value.UserId == profile.UserId && !x.Key.Equals(cleanTag, StringComparison.OrdinalIgnoreCase)).ToList();
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

        await DeliverPendingEventsAsync(cleanTag);

        return new RegisterResultDto { Success = true, Message = "OK", Profile = profile };
    }

    private async Task DeliverPendingEventsAsync(string cleanTag)
    {
        if (PendingMessages.TryGetValue(cleanTag, out var pendingList))
        {
            List<NetworkMessageDto> snapshot;
            lock (pendingList)
            {
                snapshot = pendingList.ToList();
            }

            foreach (var msg in snapshot)
            {
                UsersDb.TryGetValue(msg.SenderTag, out var senderProfile);
                await Clients.Caller.SendAsync("ReceiveRichMessage", msg, senderProfile);
            }
        }

        if (OfflineDeletions.TryRemove(cleanTag, out var pendingDeletes))
        {
            foreach (var del in pendingDeletes)
            {
                await Clients.Caller.SendAsync("MessageDeleted", del.SenderTag, del.MessageId);
            }
        }

        if (OfflineReadReceipts.TryRemove(cleanTag, out var readers))
        {
            foreach (var readerTag in readers)
            {
                await Clients.Caller.SendAsync("MessagesRead", readerTag);
            }
        }
    }

    public Task<UserProfileDto?> FindUser(string searchTag)
    {
        string cleanTag = NormalizeTag(searchTag);
        if (string.IsNullOrEmpty(cleanTag))
            return Task.FromResult<UserProfileDto?>(null);

        if (UsersDb.TryGetValue(cleanTag, out var user) && !user.IsPrivate)
        {
            return Task.FromResult<UserProfileDto?>(user);
        }

        return Task.FromResult<UserProfileDto?>(null);
    }

    public async Task SendRichMessage(NetworkMessageDto message)
    {
        string cleanSender = NormalizeTag(message.SenderTag);
        string cleanTarget = NormalizeTag(message.TargetTag);
        if (string.IsNullOrEmpty(cleanSender) || string.IsNullOrEmpty(cleanTarget)) return;

        if (string.IsNullOrEmpty(message.MessageId))
            message.MessageId = Guid.NewGuid().ToString("N");

        message.SenderTag = cleanSender;
        message.TargetTag = cleanTarget;

        // 1. Всегда кладем сообщение в очередь до получения подтверждения (AckMessage) от второго телефона
        var list = PendingMessages.GetOrAdd(cleanTarget, _ => new List<NetworkMessageDto>());
        lock (list)
        {
            if (!list.Any(m => m.MessageId == message.MessageId))
            {
                list.Add(message);
            }
        }
        SavePendingMessages();

        Console.WriteLine($"[КОНСОЛЬ СЕРВЕРА] [{message.MessageType}] @{cleanSender} -> @{cleanTarget}");

        UsersDb.TryGetValue(cleanSender, out var senderProfile);

        // 2. Если получатель сейчас подключен — сразу пушим ему сообщение
        if (OnlineUsers.TryGetValue(cleanTarget, out string? targetConnectionId))
        {
            await Clients.Client(targetConnectionId).SendAsync("ReceiveRichMessage", message, senderProfile);
        }
    }

    public async Task MarkChatAsRead(string readerTag, string messageSenderTag)
    {
        string cleanReader = NormalizeTag(readerTag);
        string cleanSender = NormalizeTag(messageSenderTag);
        if (string.IsNullOrEmpty(cleanReader) || string.IsNullOrEmpty(cleanSender)) return;

        if (OnlineUsers.TryGetValue(cleanSender, out string? senderConnectionId))
        {
            await Clients.Client(senderConnectionId).SendAsync("MessagesRead", cleanReader);
        }
        else
        {
            var set = OfflineReadReceipts.GetOrAdd(cleanSender, _ => new HashSet<string>(StringComparer.OrdinalIgnoreCase));
            lock (set)
            {
                set.Add(cleanReader);
            }
        }
    }

    public async Task DeleteMessageForEveryone(string senderTag, string targetTag, string messageId)
    {
        string cleanSender = NormalizeTag(senderTag);
        string cleanTarget = NormalizeTag(targetTag);

        if (PendingMessages.TryGetValue(cleanTarget, out var pendingList))
        {
            lock (pendingList)
            {
                pendingList.RemoveAll(m => m.MessageId == messageId);
            }
            SavePendingMessages();
        }

        if (OnlineUsers.TryGetValue(cleanTarget, out string? targetConnectionId))
        {
            await Clients.Client(targetConnectionId).SendAsync("MessageDeleted", cleanSender, messageId);
        }
        else
        {
            var delList = OfflineDeletions.GetOrAdd(cleanTarget, _ => new List<(string, string)>());
            lock (delList)
            {
                delList.Add((cleanSender, messageId));
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
