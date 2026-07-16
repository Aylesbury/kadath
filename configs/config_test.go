package configs

import "testing"

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name:    "connector mode with DSN",
			cfg:     Config{Mode: ModeConnector, AuthToken: "t", DSN: "postgresql://u:p@h:5432/db"},
			wantErr: false,
		},
		{
			name:    "connector mode without DSN",
			cfg:     Config{Mode: ModeConnector, AuthToken: "t"},
			wantErr: true,
		},
		{
			name:    "admin mode without DSN",
			cfg:     Config{Mode: ModeAdmin, AuthToken: "t"},
			wantErr: false,
		},
		{
			name:    "platform alias",
			cfg:     Config{Mode: "platform", AuthToken: "t"},
			wantErr: false,
		},
		{
			name:    "admin mode with DSN",
			cfg:     Config{Mode: ModeAdmin, AuthToken: "t", DSN: "postgresql://u:p@h:5432/db"},
			wantErr: true,
		},
		{
			name:    "missing auth token",
			cfg:     Config{Mode: ModeAdmin},
			wantErr: true,
		},
		{
			name:    "unknown mode",
			cfg:     Config{Mode: "sideways", AuthToken: "t"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestIsAdmin(t *testing.T) {
	for mode, want := range map[string]bool{
		ModeAdmin: true, "platform": true, ModeConnector: false, "": false,
	} {
		cfg := Config{Mode: mode}
		if cfg.IsAdmin() != want {
			t.Errorf("IsAdmin() with mode %q = %v, want %v", mode, cfg.IsAdmin(), want)
		}
	}
}
