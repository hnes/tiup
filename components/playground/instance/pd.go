// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package instance

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	"github.com/pingcap/errors"
	tiupexec "github.com/pingcap/tiup/pkg/exec"
	"github.com/pingcap/tiup/pkg/utils"
)

type PDMicroConfig struct {
	APIFlag             bool `json:"api,omitempty"`
	ResourceManagerFlag bool `json:"resource-manager,omitempty"`
	TSOFlag             bool `json:"tso,omitempty"`
	Amount              int  `json:"amount,omitempty"`
}

func (m *PDMicroConfig) IsNone() bool {
	return !m.APIFlag && !m.TSOFlag && !m.ResourceManagerFlag
}

func (m *PDMicroConfig) IsTSOOnly() bool {
	return !m.APIFlag && m.TSOFlag && !m.ResourceManagerFlag
}

func (m *PDMicroConfig) IsAPIAndResourceManagerMode() bool {
	return m.APIFlag && !m.TSOFlag && m.ResourceManagerFlag
}

func (m *PDMicroConfig) IsContainAPIMode() bool {
	return m.APIFlag
}

func (m *PDMicroConfig) IsContainResourceManagerMode() bool {
	return m.ResourceManagerFlag
}

func (m *PDMicroConfig) IsContainTSOMode() bool {
	return m.TSOFlag
}

func ParsePDMicroConfig(s string) ([]PDMicroConfig, error) {
	var result []PDMicroConfig
	if len(s) <= 0 {
		return result, nil
	}
	err := json.Unmarshal([]byte(s), &result)
	if err == nil {
		for _, cfg := range result {
			if cfg.Amount <= 0 {
				err = fmt.Errorf("cfg '%s' got a amount <= 0", s)
				result = nil
				break
			}
		}
	}
	return result, err
}

func MustParsePDMicroConfig(s string) []PDMicroConfig {
	result, err := ParsePDMicroConfig(s)
	if err != nil {
		log.Fatalf("parse pd micro config failed: %v", err)
	}
	return result
}

// PDInstance represent a running pd-server
type PDInstance struct {
	instance
	initEndpoints []*PDInstance
	joinEndpoints []*PDInstance
	Process
	micro PDMicroConfig
}

// NewPDInstance return a PDInstance
func NewPDInstance(binPath, dir, host, configPath string, id, port int, micro PDMicroConfig) *PDInstance {
	if port <= 0 {
		port = 2379
	}
	return &PDInstance{
		instance: instance{
			BinPath:    binPath,
			ID:         id,
			Dir:        dir,
			Host:       host,
			Port:       utils.MustGetFreePort(host, 2380),
			StatusPort: utils.MustGetFreePort(host, port),
			ConfigPath: configPath,
		},
		micro: micro,
	}
}

// Join set endpoints field of PDInstance
func (inst *PDInstance) Join(pds []*PDInstance) *PDInstance {
	inst.joinEndpoints = pds
	return inst
}

// InitCluster set the init cluster instance.
func (inst *PDInstance) InitCluster(pds []*PDInstance) *PDInstance {
	inst.initEndpoints = pds
	return inst
}

// Name return the name of pd.
func (inst *PDInstance) Name() string {
	return fmt.Sprintf("pd-%d", inst.ID)
}

// Start calls set inst.cmd and Start
func (inst *PDInstance) Start(ctx context.Context, version utils.Version) error {
	//fmt.Println("PDInstance start", inst.micro.APIFlag, inst.micro.ResourceManagerFlag, inst.micro.TSOFlag)
	uid := inst.Name()
	args := []string{}
	if inst.micro.IsNone() {
		args = []string{
			"--name=" + uid,
			fmt.Sprintf("--data-dir=%s", filepath.Join(inst.Dir, "data")),
			fmt.Sprintf("--peer-urls=http://%s", utils.JoinHostPort(inst.Host, inst.Port)),
			fmt.Sprintf("--advertise-peer-urls=http://%s", utils.JoinHostPort(AdvertiseHost(inst.Host), inst.Port)),
			fmt.Sprintf("--client-urls=http://%s", utils.JoinHostPort(inst.Host, inst.StatusPort)),
			fmt.Sprintf("--advertise-client-urls=http://%s", utils.JoinHostPort(AdvertiseHost(inst.Host), inst.StatusPort)),
			fmt.Sprintf("--log-file=%s", inst.LogFile()),
		}
	} else if inst.micro.IsAPIAndResourceManagerMode() || inst.micro.IsTSOOnly() {
		mod := []byte{}
		if inst.micro.IsContainAPIMode() {
			if len(mod) > 0 {
				mod = append(mod, []byte(",api")...)
			} else {
				mod = append(mod, []byte("api")...)
			}
		}
		if inst.micro.IsContainResourceManagerMode() {
			if len(mod) > 0 {
				mod = append(mod, []byte(",resource-manager")...)
			} else {
				mod = append(mod, []byte("resource-manager")...)
			}
		}
		if inst.micro.IsContainTSOMode() {
			if len(mod) > 0 {
				mod = append(mod, []byte(",tso")...)
			} else {
				mod = append(mod, []byte("tso")...)
			}
		}
		args = append(args, []string{
			"services",
			string(mod),
			//"--name=" + uid,
			//fmt.Sprintf("--log-file=%s", inst.LogFile()),
		}...)
		if inst.micro.IsContainAPIMode() {
			args = append(args, []string{
				"--name=" + uid,
				fmt.Sprintf("--data-dir=%s", filepath.Join(inst.Dir, "data")),
				fmt.Sprintf("--peer-urls=http://%s", utils.JoinHostPort(inst.Host, inst.Port)),
				fmt.Sprintf("--advertise-peer-urls=http://%s", utils.JoinHostPort(AdvertiseHost(inst.Host), inst.Port)),
				fmt.Sprintf("--client-urls=http://%s", utils.JoinHostPort(inst.Host, inst.StatusPort)),
				fmt.Sprintf("--advertise-client-urls=http://%s", utils.JoinHostPort(AdvertiseHost(inst.Host), inst.StatusPort)),
			}...)
		} else {
			args = append(args, []string{
				//fmt.Sprintf("--data-dir=%s", filepath.Join(inst.Dir, "data")),
				fmt.Sprintf("--listen-addr=%s", utils.JoinHostPort(inst.Host, inst.Port)),
				//fmt.Sprintf("--advertise-peer-urls=http://%s", utils.JoinHostPort(AdvertiseHost(inst.Host), inst.Port)),
				//fmt.Sprintf("--client-urls=http://%s", utils.JoinHostPort(inst.Host, inst.StatusPort)),
				//fmt.Sprintf("--advertise-client-urls=http://%s", utils.JoinHostPort(AdvertiseHost(inst.Host), inst.StatusPort)),
			}...)
		}
	} else {
		log.Fatalf("playground don't support this pd micro mode yet: %v", inst.micro)
	}

	if inst.ConfigPath != "" {
		args = append(args, fmt.Sprintf("--config=%s", inst.ConfigPath))
	}

	if inst.micro.IsNone() {
		switch {
		case len(inst.initEndpoints) > 0:
			endpoints := make([]string, 0)
			for _, pd := range inst.initEndpoints {
				uid := fmt.Sprintf("pd-%d", pd.ID)
				endpoints = append(endpoints, fmt.Sprintf("%s=http://%s", uid, utils.JoinHostPort(AdvertiseHost(inst.Host), pd.Port)))
			}
			args = append(args, fmt.Sprintf("--initial-cluster=%s", strings.Join(endpoints, ",")))
		case len(inst.joinEndpoints) > 0:
			endpoints := make([]string, 0)
			for _, pd := range inst.joinEndpoints {
				endpoints = append(endpoints, fmt.Sprintf("http://%s", utils.JoinHostPort(AdvertiseHost(inst.Host), pd.Port)))
			}
			args = append(args, fmt.Sprintf("--join=%s", strings.Join(endpoints, ",")))
		default:
			return errors.Errorf("must set the init or join instances")
		}
	} else {
		if len(inst.initEndpoints) > 0 {
			if inst.micro.IsContainAPIMode() {
				endpoints := make([]string, 0)
				for _, pd := range inst.initEndpoints {
					if pd.micro.IsContainAPIMode() {
						uid := fmt.Sprintf("pd-%d", pd.ID)
						endpoints = append(endpoints, fmt.Sprintf("%s=http://%s", uid, utils.JoinHostPort(AdvertiseHost(inst.Host), pd.Port)))
					}
				}
				args = append(args, fmt.Sprintf("--initial-cluster=%s", strings.Join(endpoints, ",")))
			} else {
				args = append(args, fmt.Sprintf("--backend-endpoints=%s", strings.Join(pdEndpoints(inst.initEndpoints, true), ",")))
			}
		} else {
			log.Fatalf("not support join yet")
		}
	}
	var err error
	if inst.BinPath, err = tiupexec.PrepareBinary("pd", version, inst.BinPath); err != nil {
		return err
	}
	inst.Process = &process{cmd: PrepareCommand(ctx, inst.BinPath, args, nil, inst.Dir)}

	logIfErr(inst.Process.SetOutputFile(inst.LogFile()))
	return inst.Process.Start()
}

// Component return the component name.
func (inst *PDInstance) Component() string {
	return "pd"
}

// LogFile return the log file.
func (inst *PDInstance) LogFile() string {
	return filepath.Join(inst.Dir, "pd.log")
}

// Addr return the listen address of PD
func (inst *PDInstance) Addr() string {
	return utils.JoinHostPort(AdvertiseHost(inst.Host), inst.StatusPort)
}
