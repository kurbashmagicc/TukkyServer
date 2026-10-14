FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src
COPY ["TukkyServer.csproj", "./"]
RUN dotnet restore "./TukkyServer.csproj"
COPY . .
RUN dotnet publish "TukkyServer.csproj" -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS final
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "TukkyServer.dll"]