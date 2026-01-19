@echo off
REM Script para configurar as variaveis de ambiente do Go
REM Execute este arquivo antes de usar o Go neste projeto

REM Verifica se o Go ja esta disponivel no PATH
go.exe version >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo go already found skipping
    exit /b 0
)

REM Define o GOROOT (raiz da instalacao do Go)
set GOROOT=C:\Export\go_compiler

REM Adiciona o diretorio bin do Go ao PATH
set PATH=%GOROOT%\bin;%PATH%

REM Opcional: Define o GOPATH (usado em versoes antigas do Go)
REM Se voce quiser usar um GOPATH especifico, descomente a linha abaixo
REM set GOPATH=C:\Export\go_workspace

REM Verifica se o Go foi configurado corretamente
echo Configurando ambiente Go...
echo GOROOT=%GOROOT%
echo PATH atualizado para incluir %GOROOT%\bin
echo.
echo Verificando instalacao do Go...
go.exe version
if %ERRORLEVEL% NEQ 0 (
    echo ERRO: Nao foi possivel executar o Go. Verifique se o caminho esta correto.
    exit /b 1
)
echo.
echo Ambiente Go configurado com sucesso!
echo NOTA: Estas configuracoes sao validas apenas para esta sessao do terminal.
echo Para usar o Go, execute este arquivo antes de trabalhar no projeto.
