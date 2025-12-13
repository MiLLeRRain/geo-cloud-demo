FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src
COPY ["GeoCloudApi.csproj", "./"]
RUN dotnet restore "GeoCloudApi.csproj"
COPY . .
WORKDIR "/src/."
RUN dotnet build "GeoCloudApi.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "GeoCloudApi.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS final
WORKDIR /app
COPY --from=publish /app/publish .
EXPOSE 8080
ENV ASPNETCORE_URLS=http://+:8080
ENTRYPOINT ["dotnet", "GeoCloudApi.dll"]