package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/containers/fetchit/pkg/engine/utils"
	"github.com/containers/podman/v4/libpod/define"
	"github.com/containers/podman/v4/pkg/specgen"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/opencontainers/runtime-spec/specs-go"

	"k8s.io/klog/v2"
)

const (
	podmanAutoUpdate        = "podman-autoupdate"
	podmanAutoUpdateService = "podman-auto-update.service"
	podmanAutoUpdateTimer   = "podman-auto-update.timer"
	podmanServicePath       = "/usr/lib/systemd/system"
	systemdPathRoot         = "/etc/systemd/system"
	systemdMethod           = "systemd"
	systemdImage            = "quay.io/fetchit/fetchit-systemd-amd:latest"
)

// Systemd to place and/or enable systemd unit files on host
type Systemd struct {
	CommonMethod `mapstructure:",squash"`
	// AutoUpdateAll will start podman-auto-update.service, podman-auto-update.timer
	// on the host. With this field true, all other fields are ignored. To place unit files
	// on host and/or enable individual services, create a separate Target.Methods.Systemd
	// 'podman auto-update' updates all services running podman with the autoupdate label
	// see https://docs.podman.io/en/latest/markdown/podman-auto-update.1.html#systemd-unit-and-timer
	// TODO: update /etc/systemd/system/podman-auto-update.timer.d/override.conf with schedule
	// By default, podman will auto-update at midnight daily when this service is running
	AutoUpdateAll bool `mapstructure:"autoUpdateAll"`
	// If true, will place unit file in /etc/systemd/system/
	// If false (default) will place unit file in ~/.config/systemd/user/
	Root bool `mapstructure:"root"`
	// If true, will enable and start all systemd services from fetched unit files
	// If true, will reload and restart the services with every scheduled run
	// Implies Enable=true, will override Enable=false
	Restart bool `mapstructure:"restart"`
	// If true, will enable and start systemd services from fetched unit files
	// If false (default), will place unit file(s) in appropriate systemd path
	Enable bool `mapstructure:"enable"`
}

func (m *Systemd) GetKind() string {
	return systemdMethod
}

func (sd *Systemd) SchedInfo() SchedInfo {
	// with autoupdate, a schedule is not required
	if sd.AutoUpdateAll {
		sd.Schedule = "*/1 * * * *"
	}
	return SchedInfo{
		schedule: sd.Schedule,
		skew:     sd.Skew,
	}
}

func (sd *Systemd) Process(ctx, conn context.Context, PAT string, skew int) {
	target := sd.GetTarget()
	time.Sleep(time.Duration(skew) * time.Millisecond)
	target.mu.Lock()
	defer target.mu.Unlock()

	if sd.AutoUpdateAll && !sd.initialRun {
		return
	}
	if sd.AutoUpdateAll {
		sd.Enable = true
		sd.Root = true
		sd.Restart = false
		sd.Name = podmanAutoUpdate
	}
	tag := []string{".service"}
	if sd.Restart {
		sd.Enable = true
	}
	if sd.initialRun {
		if sd.AutoUpdateAll {
			if err := sd.MethodEngine(ctx, conn, nil, ""); err != nil {
				klog.Infof("Failed to start podman-auto-update.service: %v", err)
			}
			sd.initialRun = false
			return
		}
		err := getClone(target, PAT)
		if err != nil {
			klog.Errorf("Failed to clone repo at %s for target %s: %v", target.url, target.Name, err)
			return
		}
	}

	err := currentToLatest(ctx, conn, sd, target, &tag)
	if err != nil {
		klog.Errorf("Error moving current to latest: %v", err)
		return
	}

	sd.initialRun = false
}

func (sd *Systemd) MethodEngine(ctx context.Context, conn context.Context, change *object.Change, path string) error {
	var prev *string = nil
	if change != nil {
		if change.To.Name != "" {
			prev = &change.To.Name
		}
	}
	nonRootHomeDir := os.Getenv("HOME")
	if nonRootHomeDir == "" {
		return fmt.Errorf("Could not determine $HOME for host, must set $HOME on host machine for non-root systemd method")
	}
	var dest string
	if sd.Root {
		dest = systemdPathRoot
	} else {
		dest = filepath.Join(nonRootHomeDir, ".config", "systemd", "user")
	}
	if change != nil {
		sd.initialRun = true
	}
	return sd.systemdPodman(ctx, conn, path, dest, prev)
}

func (sd *Systemd) Apply(ctx, conn context.Context, target *Target, currentState, desiredState plumbing.Hash, targetPath string, tags *[]string) error {
	changeMap, err := applyChanges(ctx, target, currentState, desiredState, targetPath, tags)
	if err != nil {
		return err
	}
	if err := runChangesConcurrent(ctx, conn, sd, changeMap); err != nil {
		return err
	}
	return nil
}

func (sd *Systemd) systemdPodman(ctx context.Context, conn context.Context, path, dest string, prev *string) error {
	klog.Infof("Deploying systemd file(s) %s", path)
	if sd.AutoUpdateAll {
		if !sd.initialRun {
			return nil
		}
		if err := sd.enableRestartSystemdService(conn, "autoupdate", dest, podmanAutoUpdateTimer); err != nil {
			return utils.WrapErr(err, "Error running systemctl enable --now  %s", podmanAutoUpdateTimer)
		}
		return sd.enableRestartSystemdService(conn, "autoupdate", dest, podmanAutoUpdateService)
	}
	if sd.initialRun {
		ft := &FileTransfer{
			CommonMethod: CommonMethod{
				Name: sd.Name,
			},
		}
		if err := ft.fileTransferPodman(ctx, conn, path, dest, prev); err != nil {
			return utils.WrapErr(err, "Error deploying systemd %s file(s), Path: %s", sd.Name, sd.TargetPath)
		}
	}
	if !sd.Enable {
		klog.Infof("Systemd target %s successfully processed", sd.Name)
		return nil
	}
	if (sd.Enable && !sd.Restart) || sd.initialRun {
		if sd.Enable {
			return sd.enableRestartSystemdService(conn, "enable", dest, filepath.Base(path))
		}
	}
	if sd.Restart {
		return sd.enableRestartSystemdService(conn, "restart", dest, filepath.Base(path))
	}
	return nil
}

func (sd *Systemd) enableRestartSystemdService(conn context.Context, action, dest, service string) error {
	act := action
	if action == "autoupdate" {
		act = "enable"
	}
	klog.Infof("Systemd target: %s, running systemctl %s %s", sd.Name, act, service)
	if err := detectOrFetchImage(conn, systemdImage, false); err != nil {
		return err
	}

	// TODO: remove
	if sd.Root {
		os.Setenv("ROOT", "true")
	} else {
		os.Setenv("ROOT", "false")
	}
	s := specgen.NewSpecGenerator(systemdImage, false)
	runMounttmp := "/run"
	runMountsd := "/run/systemd"
	runMountc := "/sys/fs/cgroup"
	xdg := ""
	if !sd.Root {
		// need to document this for non-root usage
		// can't use user.Current because always root in fetchit container
		xdg = os.Getenv("XDG_RUNTIME_DIR")
		if xdg == "" {
			xdg = "/run/user/1000"
		}
		runMountsd = xdg + "/systemd"
	}
	s.Privileged = true
	s.PidNS = specgen.Namespace{
		NSMode: "host",
		Value:  "",
	}
	if action == "autoupdate" {
		s.Mounts = []specs.Mount{{Source: podmanServicePath, Destination: podmanServicePath, Type: define.TypeBind, Options: []string{"rw"}}, {Source: dest, Destination: dest, Type: define.TypeBind, Options: []string{"rw"}}, {Source: runMounttmp, Destination: runMounttmp, Type: define.TypeTmpfs, Options: []string{"rw"}}, {Source: runMountc, Destination: runMountc, Type: define.TypeBind, Options: []string{"ro"}}, {Source: runMountsd, Destination: runMountsd, Type: define.TypeBind, Options: []string{"rw"}}}
	} else {
		s.Mounts = []specs.Mount{{Source: dest, Destination: dest, Type: define.TypeBind, Options: []string{"rw"}}, {Source: runMounttmp, Destination: runMounttmp, Type: define.TypeTmpfs, Options: []string{"rw"}}, {Source: runMountc, Destination: runMountc, Type: define.TypeBind, Options: []string{"ro"}}, {Source: runMountsd, Destination: runMountsd, Type: define.TypeBind, Options: []string{"rw"}}}
	}
	s.Name = "systemd-" + act + "-" + service + "-" + sd.Name
	envMap := make(map[string]string)
	envMap["ROOT"] = strconv.FormatBool(sd.Root)
	envMap["SERVICE"] = service
	envMap["ACTION"] = act
	envMap["HOME"] = os.Getenv("HOME")
	if !sd.Root {
		envMap["XDG_RUNTIME_DIR"] = xdg
	}
	s.Env = envMap
	createResponse, err := createAndStartContainer(conn, s)
	if err != nil {
		return err
	}

	err = waitAndRemoveContainer(conn, createResponse.ID)
	if err != nil {
		return err
	}
	klog.Infof("Systemd target %s-%s %s complete", sd.Name, act, service)
	return nil
}
