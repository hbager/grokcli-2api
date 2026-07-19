package toolcall

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNormalizeJSONPromotesCmdToCommandForExecCommand(t *testing.T) {
	for _, name := range []string{"exec_command", "run_command", "shell", "local_shell", "Shell"} {
		got := NormalizeJSON(`{"cmd":"curl wttr.in/Changsha"}`, name)
		if !strings.Contains(got, `"command"`) {
			t.Fatalf("%s: expected command key, got %s", name, got)
		}
		if strings.Contains(got, `"cmd"`) {
			t.Fatalf("%s: cmd alias must be removed, got %s", name, got)
		}
		if !CompleteJSON(got, name) {
			t.Fatalf("%s: CompleteJSON false for %s", name, got)
		}
	}
}

func TestProjectShellArgsForceCmdEvenForExoticName(t *testing.T) {
	in := EffectiveJSON(`{"command":"curl wttr.in/Changsha"}`, "exec_command")
	out := ProjectShellArgsForClient(in, "exec_command", "cmd")
	if !strings.Contains(out, `"cmd"`) {
		t.Fatalf("expected cmd projection: %s", out)
	}
	if strings.Contains(out, `"command"`) {
		t.Fatalf("command must not remain: %s", out)
	}
}

func TestIsShellToolCoversCodexNames(t *testing.T) {
	for _, name := range []string{"exec_command", "run_command", "shell_command", "local_shell", "default_api.exec_command", "functions.Shell"} {
		if !IsShellTool(name) {
			t.Fatalf("IsShellTool(%q)=false", name)
		}
	}
}

func TestProjectShellArgsRoundTripCmd(t *testing.T) {
	// Client history may send cmd; internal normalize to command; project back to cmd.
	internal := EffectiveJSON(`{"cmd":"pwd"}`, "shell")
	var obj map[string]any
	if err := json.Unmarshal([]byte(internal), &obj); err != nil {
		t.Fatal(err)
	}
	if _, ok := obj["command"]; !ok {
		t.Fatalf("internal must use command: %s", internal)
	}
	client := ProjectShellArgsForClient(internal, "shell", "cmd")
	if !strings.Contains(client, `"cmd":"pwd"`) && !strings.Contains(client, `"cmd": "pwd"`) {
		t.Fatalf("client form: %s", client)
	}
}
