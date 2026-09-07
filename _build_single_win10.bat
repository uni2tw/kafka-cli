dotnet publish -c Release -r win-x64 -p:PublishSingleFile=true -p:IncludeNativeLibrariesForSelfExtract=yes --self-contained
if not exist "%~dp0publish" mkdir "%~dp0publish"
copy /Y "%~dp0bin\Release\net9.0\win-x64\publish\kafka-cli.exe" "%~dp0publish\kafka-cli.exe"
dotnet publish -c Release -r linux-x64 -p:PublishSingleFile=true -p:IncludeNativeLibrariesForSelfExtract=yes --self-contained
