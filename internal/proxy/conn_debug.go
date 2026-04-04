//go:build debug

package proxy

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strings"
)

const debugConnLogFile = "pgrollback-debug-connections.log"

// debugLogIncomingConn prints endpoint details and best-effort process metadata for the client side.
// Intended for local debugging only; logs are appended to pgrollback-debug-connections.log in the cwd.
func (s *Server) debugLogIncomingConn(conn net.Conn) {
	local := conn.LocalAddr().String()
	remote := conn.RemoteAddr().String()
	debugConnPrintf("local=%s remote=%s\n", local, remote)

	// PID/path resolution below is Windows-specific and best-effort.
	if runtime.GOOS != "windows" {
		return
	}

	pid, imageName, execPath, err := lookupWindowsConnOwner(local, remote)
	cmdLine := windowsPeerCommandLine(pid)
	if err != nil {
		debugConnPrintf("remote=%s peer_pid=%s peer_program=%s peer_exe=%s peer_cmdline=%s (lookup warning: %v)\n",
			remote, emptyAsUnknown(pid), emptyAsUnknown(imageName), emptyAsUnknown(execPath), cmdLine, err)
		return
	}
	debugConnPrintf("remote=%s peer_pid=%s peer_program=%s peer_exe=%s peer_cmdline=%s\n",
		remote, emptyAsUnknown(pid), emptyAsUnknown(imageName), emptyAsUnknown(execPath), cmdLine)
}

// debugConnPrintf appends a single debug line to the dedicated debug file.
func debugConnPrintf(format string, args ...any) {
	line := fmt.Sprintf("[DEBUG CONN] "+format, args...)
	f, err := os.OpenFile(debugConnLogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		// Fallback: best-effort log to standard logger so information is not lost entirely.
		log.Printf("debugConnPrintf open %s failed: %v; line=%s", debugConnLogFile, err, strings.TrimSpace(line))
		return
	}
	defer f.Close()
	_, _ = f.WriteString(line)
}

// windowsPeerCommandLine runs the equivalent of:
//
//	Get-CimInstance Win32_Process -Filter "ProcessId=<pid>" | Select-Object CommandLine
//
// Best-effort; returns a single-line string for logging.
func windowsPeerCommandLine(pid string) string {
	pid = strings.TrimSpace(pid)
	if pid == "" {
		return "unknown"
	}
	psCmd := fmt.Sprintf(`(Get-CimInstance Win32_Process -Filter "ProcessId=%s").CommandLine`, pid)
	out, err := exec.Command("powershell", "-NoProfile", "-Command", psCmd).CombinedOutput()
	if err != nil {
		return fmt.Sprintf("unknown (powershell: %v)", err)
	}
	s := strings.TrimSpace(string(out))
	if s == "" {
		return "unknown"
	}
	s = strings.ReplaceAll(s, "\r\n", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	if len(s) > 2048 {
		s = s[:2048] + "...(truncated)"
	}
	return s
}

func lookupWindowsConnOwner(local, remote string) (pid, imageName, execPath string, err error) {
	// IMPORTANT: we want the *peer* (client) process that connected to us.
	// For an accepted server-side conn, local=serverAddr and remote=clientAddr.
	// Windows TCP tables report owning process for each endpoint row, so to find the client
	// owner we first lookup the reversed direction (local=client, remote=server).
	if p, psErr := lookupPIDWithPowerShell(remote, local); psErr == nil && p != "" {
		pid = p
	} else {
		// Fallback 1: try direct direction (may return server process, but still useful if reverse is unavailable).
		if p, psErr2 := lookupPIDWithPowerShell(local, remote); psErr2 == nil && p != "" {
			pid = p
		}
	}
	if pid == "" {
		// Fallback 2: netstat parsing using reversed endpoints first.
		out, nErr := exec.Command("netstat", "-ano", "-p", "tcp").CombinedOutput()
		if nErr != nil {
			return "", "", "", fmt.Errorf("netstat error: %w (%s)", nErr, strings.TrimSpace(string(out)))
		}
		pid = findPIDForEndpoints(string(out), remote, local)
		if pid == "" {
			pid = findPIDForEndpoints(string(out), local, remote)
		}
		if pid == "" {
			return "", "", "", fmt.Errorf("no matching tcp row for local=%s remote=%s", local, remote)
		}
	}

	imageName = "unknown"
	taskOut, taskErr := exec.Command("tasklist", "/FI", "PID eq "+pid, "/FO", "CSV", "/NH").CombinedOutput()
	if taskErr == nil {
		lines := strings.Split(strings.TrimSpace(string(taskOut)), "\n")
		if len(lines) > 0 {
			first := strings.Trim(lines[0], "\" \r\n")
			if first != "" && !strings.Contains(strings.ToLower(first), "no tasks are running") {
				parts := strings.Split(strings.TrimSpace(lines[0]), "\",\"")
				if len(parts) > 0 {
					imageName = strings.Trim(parts[0], "\"")
				}
			}
		}
	}

	execPath = "unknown"
	psCmd := fmt.Sprintf(`(Get-CimInstance Win32_Process -Filter "ProcessId=%s").ExecutablePath`, pid)
	psOut, psErr := exec.Command("powershell", "-NoProfile", "-Command", psCmd).CombinedOutput()
	if psErr == nil {
		p := strings.TrimSpace(string(psOut))
		if p != "" {
			execPath = p
		}
	}

	return pid, imageName, execPath, nil
}

func lookupPIDWithPowerShell(local, remote string) (string, error) {
	escapedLocal := strings.ReplaceAll(local, "'", "''")
	escapedRemote := strings.ReplaceAll(remote, "'", "''")
	psCmd := fmt.Sprintf(`$c=Get-NetTCPConnection -State Established -ErrorAction SilentlyContinue | Where-Object { "$($_.LocalAddress):$($_.LocalPort)" -eq '%s' -and "$($_.RemoteAddress):$($_.RemotePort)" -eq '%s' } | Select-Object -First 1; if ($c) { "$($c.OwningProcess)" }`, escapedLocal, escapedRemote)
	out, err := exec.Command("powershell", "-NoProfile", "-Command", psCmd).CombinedOutput()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func findPIDForEndpoints(netstatOutput, local, remote string) string {
	lines := strings.Split(netstatOutput, "\n")
	for _, line := range lines {
		fields := strings.Fields(strings.TrimSpace(line))
		// Typical row: TCP local remote state pid
		if len(fields) < 5 || !strings.EqualFold(fields[0], "TCP") {
			continue
		}
		lAddr, rAddr := fields[1], fields[2]
		if endpointEquals(lAddr, local) && endpointEquals(rAddr, remote) {
			return fields[len(fields)-1]
		}
	}
	return ""
}

func endpointEquals(a, b string) bool {
	normalize := func(s string) string {
		return strings.Trim(strings.ToLower(strings.TrimSpace(s)), "[]")
	}
	return normalize(a) == normalize(b)
}

func emptyAsUnknown(v string) string {
	if strings.TrimSpace(v) == "" {
		return "unknown"
	}
	return v
}

// keep io imported for future local tweaks (avoid gofmt churn if you add EOF checks etc.)
var _ io.Reader
