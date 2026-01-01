package model

import "testing"

func TestDatabaseType_IsBinlogBased(t *testing.T) {
	cases := []struct {
		name string
		dt   DatabaseType
		want bool
	}{
		{"mysql", MySQL, true},
		{"mariadb", MariaDB, true},
		{"postgres", Postgres, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.dt.IsBinlogBased(); got != tc.want {
				t.Fatalf("IsBinlogBased(%s) = %v, want %v", tc.dt, got, tc.want)
			}
		})
	}
}

func TestDatabaseType_DriverName(t *testing.T) {
	cases := []struct {
		dt   DatabaseType
		want string
	}{
		{MySQL, "mysql"},
		{MariaDB, "mysql"},
		{Postgres, "postgres"},
	}

	for _, tc := range cases {
		if got := tc.dt.DriverName(); got != tc.want {
			t.Fatalf("DriverName(%s) = %s, want %s", tc.dt, got, tc.want)
		}
	}
}

func TestDatabaseType_DriverName_Panic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic, got none")
		}
	}()

	var invalid DatabaseType = "oracle"
	_ = invalid.DriverName()
}

func TestDatabaseType_BinlogFlavor(t *testing.T) {
	cases := []struct {
		dt   DatabaseType
		want string
	}{
		{MySQL, "mysql"},
		{MariaDB, "mariadb"},
	}

	for _, tc := range cases {
		if got := tc.dt.BinlogFlavor(); got != tc.want {
			t.Fatalf("BinlogFlavor(%s) = %s, want %s", tc.dt, got, tc.want)
		}
	}
}
