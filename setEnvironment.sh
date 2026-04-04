# Define directories
export SW_MINGW64="/c/msys64"
export UPDATED_GIT="/c/Users/danilo/AppData/Local/GitHubDesktop/app-3.5.4/resources/app/git/cmd"
export GNUTOOLS="/c/Export/MappiaExplorer/deploy_apps/gnu"
export GOROOT="/c/Export/go_compiler"

# Add Go and MinGW64 compilers (for cgo) and other tools to PATH
export PATH="$GOROOT/bin:$SW_MINGW64/mingw64/bin:$GNUTOOLS:$UPDATED_GIT:$PATH"

# Enable cgo and use 64-bit MinGW GCC
export CGO_ENABLED=1
export CC=x86_64-w64-mingw32-gcc
export CXX=x86_64-w64-mingw32-g++
export GOARCH=amd64
