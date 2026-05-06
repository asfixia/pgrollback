@echo off
REM Script para configurar as variaveis de ambiente do Go e MinGW64 (cgo)
REM Execute este arquivo antes de usar o Go neste projeto, ou use o perfil "Go+MinGW" no terminal do Cursor

set sw_mingw64=C:\msys64
set MINGW64_BIN=%sw_mingw64%\mingw64\bin
set updatedGit=C:\Users\danilo\AppData\Local\GitHubDesktop\app-3.5.4\resources\app\git\cmd
set gnutools=C:\Export\MappiaExplorer\deploy_apps\gnu
set GOROOT=C:\Export\go_compiler

REM Add Go, MinGW64 compilers (for cgo), and other tools to PATH (order matches launch.json: MinGW64 then Go)
set PATH=%MINGW64_BIN%;%GOROOT%\bin;%gnutools%;%updatedGit%;%PATH%

REM So Cursor/Go finds the 64-bit MinGW gcc for cgo
set CGO_ENABLED=1
set CC=x86_64-w64-mingw32-gcc
set CXX=x86_64-w64-mingw32-g++

REM pgrollback / tests (same as .vscode launch.json "Debug PGRB Server" env)
set POSTGRES_HOST=pgdev
set POSTGRES_PORT=5433
set POSTGRES_DB=cit
set POSTGRES_USER=postgres
set POSTGRES_PASSWORD=postgres
set PGROLLBACK_LISTEN_PORT=5433
set PGROLLBACK_LISTEN_HOST=localhost
set PGROLLBACK_TIMEOUT=15s
set PGROLLBACK_LOG_LEVEL=info
set PGROLLBACK_LOG_FILE=
set PGROLLBACK_TEST_SCHEMA=_danilo
set PGROLLBACK_KEEPALIVE_INTERVAL=30s
set PGROLLBACK_TEST_CONTEXT_TIMEOUT=10s
set PGROLLBACK_TEST_QUERY_TIMEOUT=5s
set PGROLLBACK_TEST_PING_TIMEOUT=3s
set "PGROLLBACK_CONFIG=%~dp0config\pgrollback.yaml"
set GOMAXPROCS=1
set GO_TEST_VERBOSE=1

REM Verify Go (optional)
go.exe version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo AVISO: go nao encontrado. Verifique GOROOT=%GOROOT%
) else (
    echo Go + MinGW64 configurado. CC=%CC%
)

REM Opcional: Define o GOPATH (usado em versoes antigas do Go)
REM Se voce quiser usar um GOPATH especifico, descomente a linha abaixo
REM set GOPATH=C:\Export\go_workspace

REM Verifica se o Go foi configurado corretamente
REM echo Configurando ambiente Go...
REM echo GOROOT=%GOROOT%
REM echo PATH atualizado para incluir %GOROOT%\bin
REM echo.
REM echo Verificando instalacao do Go...
go.exe version
if %ERRORLEVEL% NEQ 0 (
    echo ERRO: Nao foi possivel executar o Go. Verifique se o caminho esta correto.
    exit /b 1
)
REM echo.
REM echo Ambiente Go configurado com sucesso!
REM echo NOTA: Estas configuracoes sao validas apenas para esta sessao do terminal.
REM echo Para usar o Go, execute este arquivo antes de trabalhar no projeto.

go env -w CGO_ENABLED=1
go env -w CC=x86_64-w64-mingw32-gcc
go env -w CXX=x86_64-w64-mingw32-g++

rem echo No Cursor: abra um novo terminal para carregar Go + MinGW64 (cgo).