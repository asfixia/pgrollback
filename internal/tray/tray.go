package tray

import (
	"fmt"
	"net/http"
	"os/exec"
	"runtime"
	"strings"
	"unicode/utf8"

	"github.com/getlantern/systray"
)

// Run starts the system tray icon with menu items.
// It blocks until the user selects "Quit" from the tray menu.
// postgresConnMasked is the real-database libpq-style string with password masked (e.g. host=h port=5432 dbname=d user=u password=******);
// it appears in the tray tooltip (with title and hints; truncated on Windows) and in a menu row tooltip (full text).
// onQuit is called when the user quits (use it to stop the server).
func Run(guiURL string, postgresConnMasked string, onQuit func()) {
	systray.Run(func() {
		onReady(guiURL, postgresConnMasked)
	}, func() {
		if onQuit != nil {
			onQuit()
		}
	})
}

// trayTooltipMaxRunes keeps NOTIFYICONDATA.szTip within 128 UTF-16 slots (ASCII ≈ 1 slot each).
const trayTooltipMaxRunes = 127

func truncateRunes(s string, max int) string {
	if max <= 0 || s == "" {
		return s
	}
	n := utf8.RuneCountInString(s)
	if n <= max {
		return s
	}
	return string([]rune(s)[:max-1]) + "…"
}

func trayTooltip(proxyAddr, postgresConnMasked string) string {
	line4 := fmt.Sprintf("Listening on: %s", proxyAddr)
	return "PgRollback Postgres Sandbox\n" + line4
}

func menuTitleForBackendConn(postgresConnMasked string) string {
	if strings.TrimSpace(postgresConnMasked) == "" {
		return "Backend PostgreSQL: (not configured)"
	}
	const maxTitle = 52
	t := truncateRunes(postgresConnMasked, maxTitle)
	if utf8.RuneCountInString(postgresConnMasked) > maxTitle {
		t += "…"
	}
	return "Backend: " + t
}

// proxyAddressFromGUIURL returns "host:port" from a GUI URL like "http://localhost:5433/" or "http://127.0.0.1:5433".
func proxyAddressFromGUIURL(guiURL string) string {
	s := strings.TrimSpace(guiURL)
	if i := strings.Index(s, "://"); i >= 0 {
		s = s[i+3:]
	}
	if j := strings.Index(s, "/"); j >= 0 {
		s = s[:j]
	}
	if s == "" {
		return "localhost:5432"
	}
	return s
}

func onReady(guiURL string, postgresConnMasked string) {
	systray.SetOnTrayLeftDoubleClick(func() {
		openBrowser(guiURL)
	})
	// Custom icon built at runtime (PostgreSQL-inspired blue with \"PT\" letters).
	systray.SetIcon(generateIconBase64())
	systray.SetTitle("PgRollback")
	proxyAddr := proxyAddressFromGUIURL(guiURL)
	systray.SetTooltip(trayTooltip(proxyAddr, postgresConnMasked))

	mOpen := systray.AddMenuItem(fmt.Sprintf("Open GUI \"%s\"", guiURL), "Open PgRollback GUI in the browser")
	mRollbackAll := systray.AddMenuItem("Rollback All", "Rollback all sessions (keep connections)")
	mDisconnectAll := systray.AddMenuItem("Disconnect All", "Disconnect all clients and destroy sessions")
	systray.AddSeparator()
	mProxy := systray.AddMenuItem("PostgreSQL proxy: "+proxyAddr, "Address to use as host:port in your app")
	mProxy.Disable()
	backendTip := postgresConnMasked
	if strings.TrimSpace(backendTip) == "" {
		backendTip = "Real database connection string (masked password) is not available."
	}
	mBackend := systray.AddMenuItem(menuTitleForBackendConn(postgresConnMasked), backendTip)
	mBackend.Disable()
	mCopy := systray.AddMenuItem("Copy connection URL", "Copy host=... port=... to clipboard")
	systray.AddSeparator()
	mQuit := systray.AddMenuItem("Quit", "Stop PgRollback and exit")

	connectionLine := connectionString(proxyAddr)

	baseAPI := strings.TrimSuffix(guiURL, "/")
	rollbackAllURL := baseAPI + "/api/sessions/rollback-all"
	disconnectAllURL := baseAPI + "/api/sessions/disconnect-all"
	go func() {
		for {
			select {
			case <-mOpen.ClickedCh:
				openBrowser(guiURL)
			case <-mRollbackAll.ClickedCh:
				_, _ = http.Post(rollbackAllURL, "application/json", nil)
			case <-mDisconnectAll.ClickedCh:
				_, _ = http.Post(disconnectAllURL, "application/json", nil)
			case <-mCopy.ClickedCh:
				copyToClipboard(connectionLine)
			case <-mQuit.ClickedCh:
				systray.Quit()
				return
			}
		}
	}()
}

// connectionString returns a short DSN line for the proxy (host=... port=...).
func connectionString(proxyAddr string) string {
	host, port := proxyAddr, "5432"
	if i := strings.LastIndex(proxyAddr, ":"); i >= 0 && i < len(proxyAddr)-1 {
		host = proxyAddr[:i]
		port = proxyAddr[i+1:]
	}
	return fmt.Sprintf("host=%s port=%s", host, port)
}

// copyToClipboard writes text to the system clipboard (best-effort).
func copyToClipboard(text string) {
	switch runtime.GOOS {
	case "windows":
		cmd := exec.Command("powershell", "-NoProfile", "-Command", "Set-Clipboard -Value "+escapePS(text))
		_ = cmd.Run()
		return
	case "darwin":
		cmd := exec.Command("pbcopy")
		cmd.Stdin = strings.NewReader(text)
		_ = cmd.Run()
		return
	default:
		cmd := exec.Command("xclip", "-selection", "clipboard")
		cmd.Stdin = strings.NewReader(text)
		if err := cmd.Run(); err == nil {
			return
		}
		cmd = exec.Command("xsel", "--clipboard", "--input")
		cmd.Stdin = strings.NewReader(text)
		_ = cmd.Run()
	}
}

func escapePS(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// openBrowser opens the given URL in the user's default browser.
func openBrowser(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	case "darwin":
		cmd = exec.Command("open", url)
	default: // linux, freebsd, etc.
		cmd = exec.Command("xdg-open", url)
	}
	_ = cmd.Start()
}
