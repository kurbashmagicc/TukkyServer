using Microsoft.AspNetCore.SignalR;
using System.Collections.Concurrent;

namespace TukkyServer.Hubs;

public class ChatHub : Hub
{
    // Хранилище подключений в оперативной памяти: "Ник" -> "ID соединения"
    private static readonly ConcurrentDictionary<string, string> OnlineUsers = new();

    // Регистрация ника при входе в приложение
    public async Task ConnectUser(string username)
    {
        OnlineUsers[username] = Context.ConnectionId;
        await Clients.Caller.SendAsync("ServerMessage", $"Привет, {username}! Ты в сети.");
    }

    // Отправка сообщения конкретному пользователю по нику
    public async Task SendMessageToUser(string senderUsername, string targetUsername, string messageText)
    {
        if (OnlineUsers.TryGetValue(targetUsername, out string? targetConnectionId))
        {
            await Clients.Client(targetConnectionId).SendAsync("ReceiveMessage", senderUsername, messageText);
        }
        else
        {
            await Clients.Caller.SendAsync("ServerMessage", $"Пользователь {targetUsername} сейчас не в сети.");
        }
    }

    public override Task OnDisconnectedAsync(Exception? exception)
    {
        var user = OnlineUsers.FirstOrDefault(x => x.Value == Context.ConnectionId);
        if (user.Key != null)
        {
            OnlineUsers.TryRemove(user.Key, out _);
        }
        return base.OnDisconnectedAsync(exception);
    }
}