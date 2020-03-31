
FROM mcr.microsoft.com/dotnet/core/sdk:2.2-stretch AS build
WORKDIR /app

# Copiar csproj e restaurar dependencias
COPY *.csproj ./
RUN dotnet restore

# Build da aplicacao
COPY . ./
RUN dotnet publish -c Release -o out

# Build da imagem
FROM build AS publish
WORKDIR /app
COPY --from=build /app/out .
ENTRYPOINT ["dotnet", "ProcessNewOrder.dll"]